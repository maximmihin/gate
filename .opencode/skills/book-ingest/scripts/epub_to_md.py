#!/usr/bin/env python3
"""Ingest an EPUB book into an agent-friendly markdown library.

Usage:
    python3 epub_to_md.py <book.epub> <out_dir> --slug <book-slug> \
        [--title "..."] [--authors "A, B"] [--edition "..."] [--year 2026]

Produces inside <out_dir>:
    metadata.yaml          book-level metadata (generated)
    INDEX.md               compact chapter/section map for agents (generated)
    glossary.md            glossary as term -> definition list (if present)
    chapters/*.md          one markdown file per chapter/preface, YAML frontmatter
    assets/*.png           images referenced by the text

Skipped by design: cover, title page, dedications, copyright page, colophon,
and the back-of-book page-number index (useless without page mapping).

Only Python 3 stdlib is used, no external dependencies.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import zipfile
from datetime import date
from html.parser import HTMLParser
from pathlib import Path

TEXT = "#text"
VOID = {"img", "br", "hr", "meta", "link", "input", "col", "area", "base", "source", "wbr"}

SKIP_FILE_RE = re.compile(r"(cover|titlepage|dedication|copyright|colophon|^ix\d*)", re.I)


# --------------------------------------------------------------------------- DOM


class Node:
    __slots__ = ("tag", "attrs", "children", "data")

    def __init__(self, tag, attrs=None, data=None):
        self.tag = tag
        self.attrs = dict(attrs or {})
        self.children = []
        self.data = data

    def append(self, node):
        self.children.append(node)

    def __repr__(self):  # pragma: no cover - debugging aid
        return f"<{self.tag} {self.attrs}>"


class DomBuilder(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = Node("root")
        self.stack = [self.root]

    def handle_starttag(self, tag, attrs):
        node = Node(tag, attrs)
        self.stack[-1].append(node)
        if tag not in VOID:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.stack[-1].append(Node(tag, attrs))

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                break

    def handle_data(self, data):
        if data:
            self.stack[-1].append(Node(TEXT, data=data))


def parse_html(text: str) -> Node:
    builder = DomBuilder()
    builder.feed(text)
    return builder.root


def kids(node: Node, tag=None):
    return [c for c in node.children if c.tag != TEXT and (tag is None or c.tag == tag)]


def text_of(node: Node) -> str:
    """Plain text, whitespace collapsed."""
    parts = []

    def walk(n):
        if n.tag == TEXT:
            parts.append(n.data)
        for c in n.children:
            walk(c)

    walk(node)
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


# ------------------------------------------------------------------- slug helpers


def gslug(text: str) -> str:
    """GitHub-style anchor slug for a heading."""
    s = text.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"\s+", "-", s).strip("-")
    return s


def kebab(text: str, limit: int = 60) -> str:
    s = text.lower()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"[\s-]+", "-", s).strip("-")
    return s[:limit].rstrip("-")


def yaml_str(value) -> str:
    return json.dumps(str(value), ensure_ascii=False)


def yaml_list(values) -> str:
    return "[" + ", ".join(yaml_str(v) for v in values) + "]"


# --------------------------------------------------------------------- EPUB model


class Book:
    def __init__(self, epub_path: Path):
        self.zf = zipfile.ZipFile(epub_path)
        self.opf_dir = ""
        self.meta = {}
        self.spine = []  # list of (href, doc_id)
        self._parse()

    def _parse(self):
        container = self.zf.read("META-INF/container.xml").decode("utf-8")
        opf_path = re.search(r'full-path="([^"]+)"', container).group(1)
        self.opf_dir = str(Path(opf_path).parent)
        opf = self.zf.read(opf_path).decode("utf-8")

        def field(tag):
            m = re.search(rf"<dc:{tag}[^>]*>(.*?)</dc:{tag}>", opf, re.S)
            return re.sub(r"\s+", " ", m.group(1)).strip() if m else ""

        self.meta["title"] = field("title")
        self.meta["language"] = field("language")
        self.meta["publisher"] = field("publisher")
        self.meta["date"] = field("date")
        self.meta["identifier"] = field("identifier")
        self.meta["description"] = field("description")
        creators = re.findall(r"<dc:creator[^>]*>(.*?)</dc:creator>", opf, re.S)
        authors = []
        for c in creators:
            for part in re.split(r"\s+and\s+|,\s*|\s+&\s+", c):
                p = part.strip()
                if p:
                    authors.append(p)
        self.meta["authors"] = authors or ["Unknown"]

        manifest = {}
        for m in re.finditer(r'<item\s[^>]*id="([^"]+)"[^>]*href="([^"]+)"', opf):
            manifest[m.group(1)] = m.group(2)
        for m in re.finditer(r'<itemref\s[^>]*idref="([^"]+)"', opf):
            href = manifest.get(m.group(1))
            if href:
                self.spine.append(href)

    def read_doc(self, href: str) -> str:
        path = f"{self.opf_dir}/{href}" if self.opf_dir else href
        return self.zf.read(path).decode("utf-8")


def doc_kind(href: str) -> str | None:
    """Classify a spine document. None -> skip entirely."""
    base = Path(href).name.lower()
    if SKIP_FILE_RE.search(base):
        return None
    if "chapter" in base or re.search(r"^ch\d+", base):
        return "chapter"
    if "preface" in base or "foreword" in base or "introduction" in base:
        return "preface"
    if "glossary" in base:
        return "glossary"
    if "toc" in base or "nav" in base:
        return None
    return "other"  # appendix, afterword, etc. — keep


# ------------------------------------------------------------- document extraction


class Doc:
    """A single output markdown document (chapter / preface / glossary)."""

    def __init__(self, href: str, kind: str):
        self.href = href
        self.kind = kind
        self.number = None  # chapter number
        self.title = ""
        self.file_name = ""
        self.headings = []  # [(level, text, id)]
        self.sections = []  # level-2 heading texts
        self.anchors = {}  # inner id -> (file_name, slug)
        self.md = ""
        self.words = 0
        self.imgs = []


def collect_headings(doc: Doc, root: Node) -> None:
    """Pass 1: register headings and inner anchors for cross-references."""
    chapter_h1_done = False

    def walk(n, in_figure=False, in_example=False, in_aside=False):
        nonlocal chapter_h1_done
        tag = n.tag
        fig = in_figure or tag == "figure" or (tag == "div" and "figure" in n.attrs.get("class", ""))
        ex = in_example or (tag == "div" and n.attrs.get("data-type") == "example")
        aside = in_aside or tag == "aside" or (
            tag == "div" and n.attrs.get("data-type") in ("note", "warning", "sidebar", "footnotes")
        )
        if tag in ("h1", "h2", "h3", "h4", "h5", "h6") and not (fig or ex or aside):
            txt = text_of(n)
            if txt:
                # strip leading "Chapter 3." / "3.1." style labels coming as text
                txt = re.sub(r"^(Chapter|Appendix)\s+[\dA-Z]+[\.:]\s*", "", txt)
                nid = n.attrs.get("id")
                if tag == "h1" and not chapter_h1_done and doc.kind == "chapter":
                    chapter_h1_done = True
                    doc.title = txt
                else:
                    level = {"h1": 2, "h2": 3, "h3": 4}.get(tag, 5)
                    doc.headings.append((level, txt, nid))
                    if nid:
                        doc.anchors.setdefault(nid, (None, gslug(txt)))
                    if level == 2:
                        doc.sections.append(txt)
        for c in kids(n):
            walk(c, fig, ex, aside)

    walk(root)


def register_wrapper_ids(doc: Doc, root: Node) -> None:
    """Map ids on wrapper elements to the anchor that represents them in markdown.

    Rules:
    - chapter wrapper  -> anchor of the `# {title}` heading
    - section wrappers -> slug of the first heading inside
    - figure/example   -> "" (link goes to the top of the file)
    - sidebar/note/warning/footnotes -> "" (rendered as quotes, not headings)
    """
    def first_heading_slug(n) -> str:
        for c in kids(n):
            if c.tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
                txt = re.sub(r"^(Chapter|Appendix)\s+[\dA-Z]+[\.:]\s*", "", text_of(c))
                return gslug(txt)
            s = first_heading_slug(c)
            if s:
                return s
        return ""

    def put(nid: str, slug: str):
        if nid not in doc.anchors or not doc.anchors[nid][1]:
            doc.anchors[nid] = (None, slug)

    def walk(n):
        if n.tag == TEXT:
            return
        nid = n.attrs.get("id")
        if nid:
            cls = n.attrs.get("class", "")
            dtype = n.attrs.get("data-type", "")
            if dtype != "footnotes":
                if "chapter" in cls.split():
                    put(nid, gslug(doc.title))
                elif dtype in ("note", "warning", "example") or "figure" in cls.split():
                    put(nid, "")
                else:
                    put(nid, first_heading_slug(n))
        for c in kids(n):
            walk(c)

    walk(root)


# ------------------------------------------------------------------ inline render


class RenderCtx:
    def __init__(self, doc: Doc, anchor_map: dict, all_docs: dict):
        self.doc = doc
        self.anchor_map = anchor_map  # global: id -> (doc_href, file_name, slug)
        self.all_docs = all_docs  # href -> Doc
        self.imgs = []
        self.warnings = []


def collapse(s: str) -> str:
    return re.sub(r"\s+", " ", s)


def render_inline(node: Node, ctx: RenderCtx, in_pre: bool = False) -> str:
    if node.tag == TEXT:
        return node.data if in_pre else collapse(node.data)
    tag = node.tag
    attrs = node.attrs

    if tag == "a":
        dtype = attrs.get("data-type", "")
        inner = "".join(render_inline(c, ctx, in_pre) for c in node.children)
        if dtype == "indexterm":
            return ""
        if dtype == "noteref":
            frag = (attrs.get("href") or "").split("#")[-1]
            frag = re.sub(r"-marker$", "", frag)
            return f"[^{frag}]"
        if dtype == "xref" or (attrs.get("href", "").startswith("#") and dtype != "xref"):
            return resolve_xref(attrs.get("href", ""), inner, ctx)
        href = attrs.get("href", "")
        if href.startswith("http"):
            return f"[{inner.strip() or href}]({href})" if inner.strip() else inner
        if href:
            return resolve_xref(href, inner, ctx)
        return inner

    if tag in ("em", "i"):
        inner = "".join(render_inline(c, ctx, in_pre) for c in node.children).strip()
        return f"*{inner}*" if inner else ""
    if tag in ("strong", "b"):
        inner = "".join(render_inline(c, ctx, in_pre) for c in node.children).strip()
        return f"**{inner}**" if inner else ""
    if tag == "code":
        inner = "".join(render_inline(c, ctx, in_pre) for c in node.children)
        if not inner:
            return ""
        if in_pre:
            return inner  # do NOT strip: whitespace-only spans are meaningful
        inner = inner.strip()
        return f"`{inner}`" if "`" not in inner else inner
    if tag == "br":
        return "\n" if in_pre else " "
    if tag == "img":
        src = attrs.get("src", "")
        alt = collapse(attrs.get("alt", ""))
        if alt.isdigit():
            # numbered callout circles (in code and in the callout legend)
            return f"[{alt}]"
        if in_pre:
            # other markers inside program listings: render as text
            return f"[{alt}]" if alt else "[*]"
        ctx.imgs.append(src)
        return f"![{alt}]({img_out_path(src)})"
    if tag in ("sub", "sup", "span", "small"):
        return "".join(render_inline(c, ctx, in_pre) for c in node.children)
    # generic container: render children
    return "".join(render_inline(c, ctx, in_pre) for c in node.children)


def img_out_path(src: str) -> str:
    return f"../assets/{Path(src).name}"


def resolve_xref(href: str, text: str, ctx: RenderCtx) -> str:
    """Resolve an internal cross-reference to a markdown link."""
    href = href or ""
    if "#" in href:
        file_part, frag = href.split("#", 1)
    else:
        file_part, frag = href, ""
    frag = re.sub(r"-marker$", "", frag)
    entry = ctx.anchor_map.get(frag) if frag else None
    if entry is None and file_part:
        # link to a whole document
        target = next((d for h, d in ctx.all_docs.items() if Path(h).name == Path(file_part).name), None)
        if target:
            return f"[{text.strip() or target.title}]({target.file_name})"
        ctx.warnings.append(f"unresolved xref to file: {href}")
        return text
    if entry is None:
        ctx.warnings.append(f"unresolved xref: {href}")
        return text
    target_href, target_file, slug = entry
    if target_href == ctx.doc.href:
        link = f"#{slug}" if slug else ""
    else:
        link = target_file + (f"#{slug}" if slug else "")
    if not link or link == "#":
        return text  # no useful anchor — keep the label as plain text
    return f"[{text.strip() or 'see'}]({link})"


# ------------------------------------------------------------------ block render


def render_blocks(node: Node, ctx: RenderCtx) -> list[str]:
    out = []
    for c in kids(node):
        out.extend(render_block(c, ctx))
    return out


def render_block(node: Node, ctx: RenderCtx) -> list[str]:
    tag = node.tag
    attrs = node.attrs
    dtype = attrs.get("data-type", "")

    if tag == TEXT:
        return []

    if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
        txt = text_of(node)
        if not txt:
            return []
        # the chapter opener h1 ("Chapter 3. ...") duplicates the md title line
        if re.match(r"^(Chapter|Appendix|Part)\s+[\dA-Z]+[\.:]", txt):
            return []
        # captions ("Figure 3-1. ...") render as emphasised lines, not headings
        if re.match(r"^(Figure|Example|Table)\s+\d", txt):
            return [f"*{txt}*"]
        level = {"h1": 2, "h2": 3, "h3": 4}.get(tag, 5)
        return [f"{'#' * level} {txt}"]

    if tag == "img":
        src = attrs.get("src", "")
        ctx.imgs.append(src)
        alt = collapse(attrs.get("alt", ""))
        return [f"![{alt}]({img_out_path(src)})"]

    if tag == "p":
        if dtype == "footnote":
            return render_footnote_def(node, ctx)
        if dtype == "attribution":
            return []  # handled by blockquote
        txt = "".join(render_inline(c, ctx) for c in node.children).strip()
        if not txt:
            return []
        if re.match(r"^(#|\*|-|\+|>|\d+[.)]\s)", txt):
            txt = "\\" + txt
        return [txt]

    if tag in ("ol", "ul"):
        return render_list(node, ctx)

    if tag == "li":
        return render_blocks(node, ctx)

    if tag == "blockquote":
        return render_blockquote(node, ctx)

    if tag == "pre":
        return [render_pre(node, ctx)]

    if tag == "figure":
        return render_figure_children(node, ctx)

    if tag == "table":
        return [render_table(node, ctx)]

    if tag in ("dl",):
        return render_dl(node, ctx)

    if tag in ("dt", "dd", "thead", "tbody", "tr", "figcaption"):
        return render_blocks(node, ctx)

    if tag == "aside" or (tag == "div" and dtype):
        return render_special_div(node, ctx, dtype)

    if tag == "section" or tag == "div" or tag in ("header", "footer"):
        return render_blocks(node, ctx)

    # fallback: render children
    return render_blocks(node, ctx)


def render_list(node: Node, ctx: RenderCtx) -> list[str]:
    ordered = node.tag == "ol"
    lines = []
    for i, li in enumerate(kids(node, "li"), 1):
        blocks = render_blocks(li, ctx)
        if not blocks:
            continue
        marker = f"{i}. " if ordered else "- "
        first, *rest = blocks
        lines.append((marker + first).strip())
        for b in rest:
            lines.extend("    " + ln if ln else ln for ln in b.split("\n"))
    return ["\n".join(lines)] if lines else []


def render_blockquote(node: Node, ctx: RenderCtx) -> list[str]:
    lines = []
    for c in kids(node):
        if c.tag == "p" and c.attrs.get("data-type") == "attribution":
            attr = text_of(c)
            if attr:
                lines.append(f"— *{attr}*")
        elif c.tag == "p":
            txt = "".join(render_inline(ch, ctx) for ch in c.children).strip()
            if txt:
                lines.append(txt)
        else:
            for b in render_block(c, ctx):
                lines.append(b)
    if not lines:
        return []
    return ["> " + ln for ln in lines]


def render_pre(node: Node, ctx: RenderCtx) -> str:
    raw = "".join(render_inline(c, ctx, in_pre=True) for c in node.children)
    raw = raw.replace("\xa0", " ").rstrip()
    lang = node.attrs.get("data-code-language", "")
    fence = "```"
    while fence in raw:
        fence += "`"
    return f"{fence}{lang}\n{raw}\n{fence}"


def render_figure_children(node: Node, ctx: RenderCtx) -> list[str]:
    out = []
    for c in kids(node):
        out.extend(render_block(c, ctx))
    return out


def render_caption(node: Node, ctx: RenderCtx) -> str:
    txt = "".join(render_inline(c, ctx) for c in node.children).strip()
    return f"*{txt}*" if txt else ""


def render_special_div(node: Node, ctx: RenderCtx, dtype: str) -> list[str]:
    """asides and divs with data-type: note / warning / sidebar / example / footnotes."""
    if dtype == "footnotes":
        return []  # collected separately
    if dtype in ("note", "warning"):
        title = "Warning" if dtype == "warning" else "Note"
        body_lines = []
        for c in kids(node):
            if c.tag in ("h5", "h6"):
                t = text_of(c)
                if t:
                    title = t
            else:
                for b in render_block(c, ctx):
                    body_lines.extend(b.split("\n"))
        if not body_lines:
            return []
        out = [f"> **{title}:** {body_lines[0]}"]
        out.extend("> " + ln for ln in body_lines[1:])
        return ["\n".join(out)]

    if dtype == "sidebar":
        out = []
        title = None

        def flatten(n):
            nonlocal title
            if n.tag in ("h1", "h2", "h3", "h4", "h5", "h6") and title is None:
                t = text_of(n)
                if t:
                    title = t
                return
            if n.tag == "p":
                txt = "".join(render_inline(ch, ctx) for ch in n.children).strip()
                if txt:
                    out.append(txt)
                return
            if n.tag == "pre":
                out.append(render_pre(n, ctx))
                return
            if n.tag in ("ol", "ul"):
                for b in render_list(n, ctx):
                    out.extend(b.split("\n"))
                return
            for c in kids(n):
                flatten(c)

        for c in kids(node):
            flatten(c)
        if not out and not title:
            return []
        quote = [f"> **{title}**", ">"] if title else []
        for ln in out:
            quote.extend("> " + sub for sub in ln.split("\n"))
            quote.append(">")
        while quote and quote[-1] == ">":
            quote.pop()
        return ["\n".join(quote)] if quote else []

    if dtype == "example":
        out = []
        for c in kids(node):
            if c.tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
                cap = "".join(render_inline(ch, ctx) for ch in c.children).strip()
                if cap:
                    out.append(f"**{cap}**")
            else:
                out.extend(render_block(c, ctx))
        return out

    if dtype == "epigraph":
        return render_blockquote(node, ctx)

    # generic typed div (figure wrappers etc.)
    return render_blocks(node, ctx)


def render_footnote_def(node: Node, ctx: RenderCtx) -> list[str]:
    fid = node.attrs.get("id")
    if not fid:
        return []
    txt = collapse("".join(render_inline(c, ctx, True) for c in node.children)).strip()
    # strip the printed marker: "[1] " or "[[1]()] "
    txt = re.sub(r"^\[(?:\[\d+\]\(\))?\d*\]\s*", "", txt)
    return [f"[^{fid}]: {txt}"]


def render_table(node: Node, ctx: RenderCtx) -> str:
    rows = []
    for tr in collect_desc(node, "tr"):
        cells = []
        for cell in [c for c in kids(tr) if c.tag in ("td", "th")]:
            txt = collapse("".join(render_inline(ch, ctx) for ch in cell.children)).strip()
            cells.append(txt.replace("|", "\\|"))
        if cells:
            rows.append(cells)
    if not rows:
        return ""
    width = max(len(r) for r in rows)
    for r in rows:
        r.extend([""] * (width - len(r)))
    head, body = rows[0], rows[1:]
    lines = ["| " + " | ".join(head) + " |", "|" + "---|" * width]
    lines.extend("| " + " | ".join(r) + " |" for r in body)
    return "\n".join(lines)


def collect_desc(node: Node, tag: str) -> list[Node]:
    found = []

    def walk(n):
        if n.tag == tag:
            found.append(n)
        for c in kids(n):
            walk(c)

    walk(node)
    return found


def render_dl(node: Node, ctx: RenderCtx) -> list[str]:
    lines_out = []
    term = None
    for c in kids(node):
        if c.tag == "dt":
            term = "".join(render_inline(ch, ctx) for ch in c.children).strip()
            term = re.sub(r"^\[(\d+)\]$", r"\1", term)  # callout circle markers
        elif c.tag == "dd" and term is not None:
            blocks = render_blocks(c, ctx)
            if blocks:
                # indent every continuation line so the bullet stays intact
                lines = []
                for bi, b in enumerate(blocks):
                    for j, ln in enumerate(b.split("\n")):
                        if bi == 0 and j == 0:
                            lines.append(ln)
                        else:
                            lines.append(("  " + ln) if ln else ln)
                lines[0] = f"- **{term}** — {lines[0]}"
                lines_out.extend(lines)
            term = None
    return ["\n".join(lines_out)] if lines_out else []


# ---------------------------------------------------------------- document render


def render_document(doc: Doc, root: Node, ctx: RenderCtx) -> tuple[str, list[str]]:
    """Return (body_markdown, footnote_defs)."""
    body_nodes = [c for c in kids(root) if not (c.tag == "div" and c.attrs.get("data-type") == "footnotes")]
    container = Node("root")
    container.children = body_nodes
    blocks = render_blocks(container, ctx)

    footnotes = []
    for div in collect_desc(root, "div"):
        if div.attrs.get("data-type") == "footnotes":
            for b in render_blocks(div, ctx):
                footnotes.extend(ln for ln in b.split("\n") if ln.startswith("[^"))

    body = "\n\n".join(blocks)
    # footnote refs grouped in brackets by the publisher: "[[^a], [^b]]" -> "[^a], [^b]"
    body = re.sub(r"\[((?:\[\^[A-Za-z0-9_-]+\](?:,\s*)?)+)\]", r"\1", body)
    body = re.sub(r"\n{3,}", "\n\n", body).strip()
    doc.words = len(body.split())
    return body, footnotes


# ------------------------------------------------------------------------ output


def build_frontmatter(doc: Doc, book_args: dict) -> str:
    fields = [
        ("book", yaml_str(book_args["slug"])),
        ("title", yaml_str(doc.title)),
    ]
    if doc.kind == "chapter" and doc.number:
        fields.append(("chapter", str(doc.number)))
    fields.append(("kind", yaml_str(doc.kind)))
    fields.append(("source", yaml_str(doc.href)))
    fields.append(("words", str(doc.words)))
    if doc.sections:
        fields.append(("sections", yaml_list(doc.sections)))
    return "---\n" + "\n".join(f"{k}: {v}" for k, v in fields) + "\n---"


def chapter_file_name(doc: Doc) -> str:
    if doc.kind == "glossary":
        return "../glossary.md"  # lives at the book root; relative to chapters/
    if doc.kind == "chapter" and doc.number:
        return f"{doc.number:02d}-{kebab(doc.title)}.md"
    if doc.kind == "preface":
        return "00-preface.md"
    return (kebab(doc.title) or doc.kind) + ".md"


def mini_toc(doc: Doc) -> str:
    items = []
    for level, txt, _nid in doc.headings:
        if level == 2:
            items.append(f"[{txt}](#{gslug(txt)})")
    if not items:
        return ""
    return "**Sections:** " + " · ".join(items)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("epub", type=Path)
    ap.add_argument("out_dir", type=Path)
    ap.add_argument("--slug", required=True)
    ap.add_argument("--title")
    ap.add_argument("--authors", help="comma-separated; default: from EPUB metadata")
    ap.add_argument("--edition")
    ap.add_argument("--year")
    args = ap.parse_args()

    book = Book(args.epub.resolve())

    title = args.title or re.sub(r"\s*\(for \.[\s.]*\)\s*$", "", book.meta["title"]).strip()
    authors = [a.strip() for a in args.authors.split(",")] if args.authors else book.meta["authors"]
    year = args.year or (book.meta["date"][:4] if book.meta["date"] else "")

    # ---- choose documents in spine order
    chosen = []
    for href in book.spine:
        kind = doc_kind(href)
        if kind:
            chosen.append(Doc(href, kind))
    if not chosen:
        print("ERROR: no suitable documents found in spine", file=sys.stderr)
        return 1

    # ---- pass 1: parse, titles, headings, anchors
    doms = {}
    for doc in chosen:
        root = parse_html(book.read_doc(doc.href))
        doms[doc.href] = root
        # chapter number: from h1 label or file name
        m = re.search(r"<h1[^>]*>\s*<span[^>]*>\s*Chapter\s+(\d+)", book.read_doc(doc.href))
        if not m:
            m = re.search(r"ch(\d+)", Path(doc.href).stem, re.I)
        if m and doc.kind == "chapter":
            doc.number = int(m.group(1))
        collect_headings(doc, root)
        register_wrapper_ids(doc, root)
        if doc.kind == "chapter" and not doc.title:
            doc.title = f"Chapter {doc.number}"
        if doc.kind == "preface" and not doc.title:
            doc.title = "Preface"
        if doc.kind == "glossary" and not doc.title:
            doc.title = "Glossary"
        doc.file_name = chapter_file_name(doc)

    # ---- global anchor map: id -> (href, file_name, slug)
    anchor_map = {}
    for doc in chosen:
        for aid, (_f, slug) in doc.anchors.items():
            if aid not in anchor_map:
                anchor_map[aid] = (doc.href, doc.file_name, slug)

    # ---- pass 2: render
    all_docs = {d.href: d for d in chosen}
    warnings = set()
    for doc in chosen:
        ctx = RenderCtx(doc, anchor_map, all_docs)
        body, footnotes = render_document(doc, doms[doc.href], ctx)
        warnings.update(f"{doc.file_name}: {w}" for w in ctx.warnings)
        doc.imgs = ctx.imgs

        parts = [build_frontmatter(doc, vars(args) | {"slug": args.slug, "title": title, "authors": authors})]
        parts.append(f"# {doc.title}")
        toc = mini_toc(doc)
        if toc:
            parts.append(toc)
        parts.append(body)
        if footnotes:
            parts.append("## Footnotes\n\n" + "\n\n".join(footnotes))
        doc.md = "\n\n".join(parts) + "\n"

    # ---- write output tree
    out = args.out_dir
    (out / "chapters").mkdir(parents=True, exist_ok=True)
    (out / "assets").mkdir(parents=True, exist_ok=True)

    for doc in chosen:
        if doc.kind == "glossary":
            continue  # written to the book root as glossary.md
        (out / "chapters" / doc.file_name).write_text(doc.md, encoding="utf-8")

    # assets
    extracted = 0
    needed = {}
    for doc in chosen:
        for src in doc.imgs:
            needed.setdefault(Path(src).name, src)
    for name, src in needed.items():
        src_path = f"{book.opf_dir}/{src}" if book.opf_dir else src
        try:
            (out / "assets" / name).write_bytes(book.zf.read(src_path))
            extracted += 1
        except KeyError:
            warnings.add(f"missing asset: {src}")

    # glossary goes to book root as a reference file
    glossary = next((d for d in chosen if d.kind == "glossary"), None)
    if glossary:
        (out / "glossary.md").write_text(glossary.md, encoding="utf-8")

    chapters = [d for d in chosen if d.kind != "glossary"]

    # metadata.yaml
    desc = book.meta.get("description", "")
    desc_lines = re.sub(r"\s+", " ", desc).strip()
    meta = [
        f"slug: {yaml_str(args.slug)}",
        f"title: {yaml_str(title)}",
        f"authors: {yaml_list(authors)}",
    ]
    if args.edition:
        meta.append(f"edition: {yaml_str(args.edition)}")
    if year:
        meta.append(f"year: {yaml_str(year)}")
    if book.meta.get("publisher"):
        meta.append(f"publisher: {yaml_str(book.meta['publisher'])}")
    if book.meta.get("identifier"):
        meta.append(f"isbn: {yaml_str(book.meta['identifier'])}")
    meta.append(f"language: {yaml_str(book.meta.get('language', 'en'))}")
    meta.append(f"source_original: {yaml_str(f'originals/{args.epub.name}')}")
    meta.append(f"ingested_at: {date.today().isoformat()}")
    meta.append("generator: .opencode/skills/book-ingest/scripts/epub_to_md.py")
    if desc_lines:
        # wrap on word boundaries
        words, lines, cur = desc_lines.split(), [], ""
        for w in words:
            if len(cur) + len(w) + 1 > 96 and cur:
                lines.append(cur)
                cur = w
            else:
                cur = f"{cur} {w}".strip()
        if cur:
            lines.append(cur)
        meta.append("description: >-")
        meta.extend("  " + w for w in lines)
    (out / "metadata.yaml").write_text("\n".join(meta) + "\n", encoding="utf-8")

    # INDEX.md
    idx = [f"# {title}", ""]
    idx.append(f"**Authors:** {', '.join(authors)}  ")
    if args.edition:
        idx.append(f"**Edition:** {args.edition}  ")
    if year:
        idx.append(f"**Year:** {year}  ")
    idx.append(f"**Publisher:** {book.meta.get('publisher', '')}  ")
    idx.append(f"**Original:** `originals/{args.epub.name}`")
    idx.append("")
    idx.append(
        "Consultation protocol: start here to locate a chapter/section, "
        "check `topics.md` for question-to-section mapping, `grep` the phrase "
        "you need inside `chapters/`, then read only the matching slice of the chapter file."
    )
    idx.append("")
    idx.append("## Chapters")
    idx.append("")
    for doc in chapters:
        heading = doc.title
        if doc.kind == "chapter" and doc.number:
            heading = f"{doc.number}. {doc.title}"
        idx.append(f"### {heading}")
        idx.append("")
        idx.append(f"File: `chapters/{doc.file_name}` — ~{doc.words:,} words")
        idx.append("")
        for level, txt, _nid in doc.headings:
            if level == 2:
                idx.append(f"- [{txt}](chapters/{doc.file_name}#{gslug(txt)})")
        idx.append("")
    refs = []
    if glossary:
        refs.append(("Glossary", "glossary.md", "key terms, one-line definitions with links into the text"))
    refs.append(("Topic map", "topics.md", 'curated "which sections answer which engineering question"'))
    idx.append("## Reference")
    idx.append("")
    for name, path, desc_r in refs:
        idx.append(f"- **{name}:** [`{path}`]({path}) — {desc_r}")
    idx.append("")
    idx.append("> `INDEX.md` and `chapters/` are generated by the book-ingest skill; "
               "do not edit them by hand — re-run the ingest script instead. "
               "`topics.md` is curated manually.")
    (out / "INDEX.md").write_text("\n".join(idx) + "\n", encoding="utf-8")

    # ---- report
    print(f"Ingested: {title}")
    print(f"  documents: {len(chosen)} ({len(chapters)} chapters/preface"
          + (", 1 glossary" if glossary else "") + ")")
    total_words = sum(d.words for d in chosen)
    print(f"  total words: {total_words:,}")
    print(f"  assets extracted: {extracted}")
    skipped = [Path(h).name for h in book.spine if not doc_kind(h)]
    if skipped:
        print(f"  skipped by design: {', '.join(skipped)}")
    if warnings:
        print("  WARNINGS:")
        for w in sorted(warnings)[:20]:
            print(f"    - {w}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
