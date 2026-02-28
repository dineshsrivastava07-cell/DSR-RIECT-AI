#!/usr/bin/env python3
"""
Convert README.md and ARCHITECTURE.md to styled PDFs using fpdf2 (pure Python).
Usage: python3 scripts/md_to_pdf.py
"""

import re
import sys
import pathlib
from fpdf import FPDF, XPos, YPos

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
C_NAVY    = (15, 52, 96)        # headings / cover
C_BLUE    = (26, 107, 181)      # h2 accent
C_MIDBLUE = (22, 33, 62)        # h3
C_SLATE   = (40, 55, 71)        # body text
C_MUTED   = (100, 110, 130)     # footer / meta
C_CODE_BG = (240, 244, 250)     # inline code bg
C_CODE_FG = (160, 32, 32)       # inline code fg
C_PRE_BG  = (26, 26, 46)        # code block bg
C_PRE_FG  = (224, 230, 240)     # code block fg
C_TH_BG   = (15, 52, 96)        # table header bg
C_TR_ODD  = (247, 249, 252)     # table odd row
C_BORDER  = (208, 218, 234)     # table border
C_HR      = (192, 202, 218)     # horizontal rule

FONT_BODY   = 9.5
FONT_H1     = 22
FONT_H2     = 14
FONT_H3     = 11
FONT_H4     = 10
FONT_CODE   = 8.5
FONT_FOOTER = 7.5
LMARGIN     = 18
RMARGIN     = 18
PAGE_W      = 210   # A4 mm

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def strip_inline(text):
    """Remove markdown inline markers for plain measurement."""
    return re.sub(r'[*_`~]+', '', text)


def _rgb(pdf, r, g, b):
    pdf.set_text_color(r, g, b)


def _fill(pdf, r, g, b):
    pdf.set_fill_color(r, g, b)


def _draw(pdf, r, g, b):
    pdf.set_draw_color(r, g, b)


# ---------------------------------------------------------------------------
# Main PDF class
# ---------------------------------------------------------------------------

class MarkdownPDF(FPDF):
    def __init__(self, doc_title="Document"):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.doc_title = doc_title
        self.set_margins(LMARGIN, 14, RMARGIN)
        self.set_auto_page_break(auto=True, margin=20)
        self.add_font("DejaVu",       style="",   fname=self._find_font("Arial.ttf"))
        self.add_font("DejaVu",       style="B",  fname=self._find_font("Arial Bold.ttf"))
        self.add_font("DejaVu",       style="I",  fname=self._find_font("Arial Italic.ttf"))
        self.add_font("DejaVu",       style="BI", fname=self._find_font("Arial Bold Italic.ttf"))
        self.add_font("DejaVuMono",   style="",   fname=self._find_font("Courier New.ttf"))
        self.add_font("DejaVuMono",   style="B",  fname=self._find_font("Courier New Bold.ttf"))
        self._in_code_block   = False
        self._code_block_lines: list[str] = []
        self._in_table        = False
        self._table_header: list[str] = []
        self._table_rows: list[list[str]] = []
        self._list_depth      = 0
        self._list_markers: list[str] = []

    # ------------------------------------------------------------------
    # Font resolver – macOS system fonts
    # ------------------------------------------------------------------
    _FONT_MAP = {
        "Arial.ttf":               "/System/Library/Fonts/Supplemental/Arial.ttf",
        "Arial Bold.ttf":          "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        "Arial Italic.ttf":        "/System/Library/Fonts/Supplemental/Arial Italic.ttf",
        "Arial Bold Italic.ttf":   "/System/Library/Fonts/Supplemental/Arial Bold Italic.ttf",
        "Courier New.ttf":         "/System/Library/Fonts/Supplemental/Courier New.ttf",
        "Courier New Bold.ttf":    "/System/Library/Fonts/Supplemental/Courier New Bold.ttf",
    }

    @staticmethod
    def _find_font(name: str) -> str:
        p = MarkdownPDF._FONT_MAP.get(name)
        if p and pathlib.Path(p).exists():
            return p
        raise FileNotFoundError(f"Font not found: {name}")

    # ------------------------------------------------------------------
    # Header / Footer
    # ------------------------------------------------------------------
    def header(self):
        if self.page_no() == 1:
            return
        self.set_font("DejaVu", "", FONT_FOOTER)
        _rgb(self, *C_MUTED)
        self.cell(0, 6, "DSR|RIECT — Retail Intelligence Execution Control Tower",
                  align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        _draw(self, *C_HR)
        self.set_draw_color(192, 202, 218)
        self.line(LMARGIN, self.get_y(), PAGE_W - RMARGIN, self.get_y())
        self.ln(2)

    def footer(self):
        self.set_y(-14)
        _draw(self, *C_HR)
        self.line(LMARGIN, self.get_y(), PAGE_W - RMARGIN, self.get_y())
        self.ln(1)
        self.set_font("DejaVu", "", FONT_FOOTER)
        _rgb(self, *C_MUTED)
        self.cell(0, 5, f"Page {self.page_no()}", align="R",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_y(self.get_y() - 5)
        self.cell(0, 5, "© 2025 Dinesh Srivastava — Confidential", align="L")

    # ------------------------------------------------------------------
    # Utility writers
    # ------------------------------------------------------------------
    def _usable_w(self):
        return PAGE_W - LMARGIN - RMARGIN

    def _write_inline(self, text: str, base_size: float = FONT_BODY,
                       color=None, bold=False, italic=False):
        """Render a line of text, honouring **bold**, *italic*, and `code` spans."""
        if color is None:
            color = C_SLATE
        # Split on inline markers
        pattern = r'(`[^`]+`|\*\*[^*]+\*\*|\*[^*]+\*|__[^_]+__|_[^_]+_)'
        parts = re.split(pattern, text)
        for part in parts:
            if not part:
                continue
            if part.startswith('`') and part.endswith('`'):
                word = part[1:-1]
                _fill(self, *C_CODE_BG)
                _rgb(self, *C_CODE_FG)
                self.set_font("DejaVuMono", "", base_size - 1)
                self.write(base_size * 0.45, word)
            elif (part.startswith('**') and part.endswith('**')):
                word = part[2:-2]
                self.set_font("DejaVu", "B", base_size)
                _rgb(self, *C_NAVY)
                self.write(base_size * 0.45, word)
            elif part.startswith('*') and part.endswith('*'):
                word = part[1:-1]
                self.set_font("DejaVu", "I", base_size)
                _rgb(self, *color)
                self.write(base_size * 0.45, word)
            elif part.startswith('__') and part.endswith('__'):
                word = part[2:-2]
                self.set_font("DejaVu", "B", base_size)
                _rgb(self, *C_NAVY)
                self.write(base_size * 0.45, word)
            elif part.startswith('_') and part.endswith('_'):
                word = part[1:-1]
                self.set_font("DejaVu", "I", base_size)
                _rgb(self, *color)
                self.write(base_size * 0.45, word)
            else:
                style = "B" if bold else ("I" if italic else "")
                self.set_font("DejaVu", style, base_size)
                _rgb(self, *color)
                # Remove leftover link markup [text](url) → text
                part = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', part)
                self.write(base_size * 0.45, part)

    def _para(self, text: str, size=FONT_BODY, color=None, bold=False, italic=False,
              indent=0, spacing=4):
        """Write a paragraph with word-wrap and optional indent."""
        if color is None:
            color = C_SLATE
        if indent:
            self.set_x(LMARGIN + indent)
        self._write_inline(text, base_size=size, color=color, bold=bold, italic=italic)
        self.ln(size * 0.45 + spacing)
        self.set_x(LMARGIN)

    def _flush_code_block(self):
        if not self._code_block_lines:
            return
        content = "\n".join(self._code_block_lines)
        self._code_block_lines = []

        padding = 4
        line_h = FONT_CODE * 0.42

        # measure height
        lines = content.split("\n")
        block_h = len(lines) * line_h + padding * 2
        if self.get_y() + block_h > self.h - 25:
            self.add_page()

        y0 = self.get_y()
        x0 = LMARGIN
        w  = self._usable_w()

        _fill(self, *C_PRE_BG)
        _draw(self, *C_NAVY)
        self.set_line_width(0.3)
        self.rect(x0, y0, w, block_h, style="FD")

        self.set_font("DejaVuMono", "", FONT_CODE)
        _rgb(self, *C_PRE_FG)
        y = y0 + padding
        for line in lines:
            self.set_xy(x0 + padding, y)
            # truncate long lines
            safe = line[:120]
            self.cell(w - padding * 2, line_h, safe)
            y += line_h

        self.set_y(y0 + block_h + 3)
        self.set_x(LMARGIN)

    def _flush_table(self):
        if not self._table_header:
            self._table_rows = []
            self._table_header = []
            return

        headers = self._table_header
        rows    = self._table_rows
        n       = len(headers)
        if n == 0:
            return

        usable = self._usable_w()
        col_w  = usable / n
        row_h  = 6.5
        hdr_h  = 7.5

        # page break check
        needed = hdr_h + len(rows) * row_h
        if self.get_y() + needed > self.h - 25:
            self.add_page()

        x0 = LMARGIN
        y0 = self.get_y()

        # header row
        _fill(self, *C_TH_BG)
        _draw(self, *C_TH_BG)
        self.set_font("DejaVu", "B", 8)
        _rgb(self, 255, 255, 255)
        for i, h in enumerate(headers):
            self.set_xy(x0 + i * col_w, y0)
            self.cell(col_w, hdr_h, strip_inline(h)[:28], border=0, fill=True, align="L")
        self.set_y(y0 + hdr_h)

        # data rows
        _draw(self, *C_BORDER)
        self.set_line_width(0.2)
        for ri, row in enumerate(rows):
            ry = self.get_y()
            if ri % 2 == 0:
                _fill(self, *C_TR_ODD)
            else:
                _fill(self, 255, 255, 255)
            _rgb(self, *C_SLATE)
            self.set_font("DejaVu", "", 8)
            for ci in range(n):
                cell_text = row[ci].strip() if ci < len(row) else ""
                cell_text = re.sub(r'\*\*([^*]+)\*\*', r'\1', cell_text)
                cell_text = re.sub(r'\*([^*]+)\*', r'\1', cell_text)
                cell_text = re.sub(r'`([^`]+)`', r'\1', cell_text)
                cell_text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', cell_text)
                self.set_xy(x0 + ci * col_w, ry)
                self.cell(col_w, row_h, cell_text[:32], border="B", fill=True, align="L")
            self.set_y(ry + row_h)

        self.ln(4)
        self.set_x(LMARGIN)
        self._table_header = []
        self._table_rows   = []

    # ------------------------------------------------------------------
    # Cover page
    # ------------------------------------------------------------------
    def cover_page(self, title: str, subtitle: str = ""):
        self.add_page()
        # Navy top band
        _fill(self, *C_NAVY)
        self.rect(0, 0, PAGE_W, 60, style="F")

        # Title
        self.set_y(15)
        self.set_font("DejaVu", "B", 26)
        _rgb(self, 255, 255, 255)
        self.cell(0, 14, title, align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        # Subtitle band
        _fill(self, 22, 33, 62)
        self.rect(0, 60, PAGE_W, 18, style="F")
        self.set_y(63)
        self.set_font("DejaVu", "", 11)
        _rgb(self, 180, 200, 230)
        self.cell(0, 12, subtitle, align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        # Meta block
        self.set_y(92)
        meta_items = [
            ("Platform",  "DSR|RIECT — Retail Intelligence Execution Control Tower"),
            ("Author",    "Dinesh Srivastava"),
            ("Version",   "v1.0  |  March 2025"),
            ("Status",    "Production-Ready"),
        ]
        self.set_font("DejaVu", "", 10)
        for label, value in meta_items:
            self.set_x(LMARGIN)
            _rgb(self, *C_MUTED)
            self.cell(38, 8, label + ":", align="L")
            _rgb(self, *C_NAVY)
            self.set_font("DejaVu", "B", 10)
            self.cell(0, 8, value, align="L", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            self.set_font("DejaVu", "", 10)

        # Bottom decorative band
        _fill(self, *C_NAVY)
        self.rect(0, 270, PAGE_W, 27, style="F")
        self.set_y(276)
        self.set_font("DejaVu", "", 8)
        _rgb(self, 160, 180, 210)
        self.cell(0, 6, "© 2025 Dinesh Srivastava — Confidential — Not for Distribution",
                  align="C")

    # ------------------------------------------------------------------
    # Core parser
    # ------------------------------------------------------------------
    def parse_markdown(self, text: str):
        lines = text.splitlines()
        i = 0
        while i < len(lines):
            line = lines[i]

            # --- code block ---
            if line.strip().startswith("```"):
                if not self._in_code_block:
                    self._in_code_block = True
                    self._code_block_lines = []
                else:
                    self._in_code_block = False
                    self._flush_code_block()
                i += 1
                continue
            if self._in_code_block:
                self._code_block_lines.append(line)
                i += 1
                continue

            # --- table ---
            if line.strip().startswith("|"):
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                if not self._in_table:
                    self._in_table = True
                    self._table_header = cells
                    self._table_rows   = []
                    i += 1
                    # skip separator row
                    if i < len(lines) and re.match(r'[\s|:-]+$', lines[i]):
                        i += 1
                    continue
                else:
                    # skip separator
                    if re.match(r'[\s|:-]+$', line.strip()):
                        i += 1
                        continue
                    self._table_rows.append(cells)
                    i += 1
                    continue
            else:
                if self._in_table:
                    self._in_table = False
                    self._flush_table()

            # --- heading ---
            h_match = re.match(r'^(#{1,4})\s+(.*)', line)
            if h_match:
                level  = len(h_match.group(1))
                h_text = h_match.group(2).strip()
                # strip inline badge/anchor
                h_text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', h_text)
                h_text = re.sub(r'[*_`]+', '', h_text)
                self._emit_heading(level, h_text)
                i += 1
                continue

            # --- horizontal rule ---
            if re.match(r'^[-*_]{3,}\s*$', line.strip()):
                _draw(self, *C_HR)
                self.set_line_width(0.4)
                y = self.get_y() + 2
                self.line(LMARGIN, y, PAGE_W - RMARGIN, y)
                self.ln(5)
                i += 1
                continue

            # --- blockquote ---
            if line.startswith(">"):
                content = line.lstrip(">").strip()
                _fill(self, 240, 244, 250)
                self.set_fill_color(240, 244, 250)
                bq_x = LMARGIN + 4
                bq_w = self._usable_w() - 4
                _fill(self, 240, 244, 250)
                self.set_x(bq_x)
                self.set_font("DejaVu", "I", FONT_BODY - 0.5)
                _rgb(self, 60, 70, 90)
                self.multi_cell(bq_w, 5.5, self._clean(content), fill=True)
                self.ln(2)
                i += 1
                continue

            # --- unordered list ---
            ul_match = re.match(r'^(\s*)([-*+])\s+(.*)', line)
            if ul_match:
                indent_spaces = len(ul_match.group(1))
                depth  = indent_spaces // 2
                item   = ul_match.group(3)
                bullet = "•" if depth == 0 else ("◦" if depth == 1 else "▸")
                x_off  = LMARGIN + depth * 5
                self.set_x(x_off)
                self.set_font("DejaVu", "", FONT_BODY)
                _rgb(self, *C_NAVY)
                self.cell(4, 5.5, bullet)
                _rgb(self, *C_SLATE)
                clean = self._clean(item)
                self.multi_cell(self._usable_w() - depth * 5 - 4, 5.5, clean)
                self.set_x(LMARGIN)
                i += 1
                continue

            # --- ordered list ---
            ol_match = re.match(r'^(\s*)\d+\.\s+(.*)', line)
            if ol_match:
                depth  = len(ol_match.group(1)) // 2
                item   = ol_match.group(2)
                x_off  = LMARGIN + depth * 5
                self.set_x(x_off)
                self.set_font("DejaVu", "", FONT_BODY)
                _rgb(self, *C_NAVY)
                self.cell(6, 5.5, f"{self._ol_count(i, lines, depth)}.")
                _rgb(self, *C_SLATE)
                clean = self._clean(item)
                self.multi_cell(self._usable_w() - depth * 5 - 6, 5.5, clean)
                self.set_x(LMARGIN)
                i += 1
                continue

            # --- blank line ---
            if line.strip() == "":
                self.ln(3)
                i += 1
                continue

            # --- normal paragraph ---
            self.set_font("DejaVu", "", FONT_BODY)
            _rgb(self, *C_SLATE)
            clean = self._clean(line)
            if clean:
                self.multi_cell(self._usable_w(), 5.5, clean)
                self.ln(1.5)
            i += 1

        # flush any pending blocks
        if self._in_code_block:
            self._flush_code_block()
        if self._in_table:
            self._flush_table()

    def _emit_heading(self, level: int, text: str):
        if level == 1:
            # Skip repeated document title (same as cover page)
            self.ln(6)
            _rgb(self, *C_NAVY)
            self.set_font("DejaVu", "B", FONT_H1)
            self.multi_cell(self._usable_w(), 10, text)
            _draw(self, *C_NAVY)
            self.set_line_width(0.8)
            y = self.get_y() + 1
            self.line(LMARGIN, y, PAGE_W - RMARGIN, y)
            self.ln(7)
        elif level == 2:
            if self.get_y() > self.h - 50:
                self.add_page()
            self.ln(5)
            _rgb(self, *C_BLUE)
            self.set_font("DejaVu", "B", FONT_H2)
            self.multi_cell(self._usable_w(), 8, text)
            _draw(self, *C_BLUE)
            self.set_line_width(0.4)
            y = self.get_y() + 0.5
            self.line(LMARGIN, y, PAGE_W - RMARGIN, y)
            self.ln(5)
        elif level == 3:
            self.ln(4)
            _rgb(self, *C_MIDBLUE)
            self.set_font("DejaVu", "B", FONT_H3)
            self.multi_cell(self._usable_w(), 7, text)
            self.ln(3)
        else:
            self.ln(3)
            _rgb(self, 44, 95, 138)
            self.set_font("DejaVu", "BI", FONT_H4)
            self.multi_cell(self._usable_w(), 6.5, text)
            self.ln(2)
        self.set_x(LMARGIN)

    @staticmethod
    def _clean(text: str) -> str:
        """Strip markdown inline syntax to plain text for multi_cell."""
        text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
        text = re.sub(r'\*([^*]+)\*',     r'\1', text)
        text = re.sub(r'__([^_]+)__',     r'\1', text)
        text = re.sub(r'_([^_]+)_',       r'\1', text)
        text = re.sub(r'`([^`]+)`',       r'\1', text)
        text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
        text = re.sub(r'~~([^~]+)~~',     r'\1', text)
        return text.strip()

    @staticmethod
    def _ol_count(pos: int, lines: list[str], depth: int) -> int:
        """Count ordered list position at this depth."""
        count = 0
        for j in range(pos + 1):
            m = re.match(r'^(\s*)\d+\.\s+', lines[j])
            if m and len(m.group(1)) // 2 == depth:
                count += 1
        return count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def convert(md_path: pathlib.Path, pdf_path: pathlib.Path,
            doc_title: str, subtitle: str) -> None:
    print(f"  Reading  : {md_path.name}")
    text = md_path.read_text(encoding="utf-8")

    pdf = MarkdownPDF(doc_title=doc_title)
    pdf.set_title(doc_title)
    pdf.set_author("Dinesh Srivastava")
    pdf.set_creator("DSR|RIECT md_to_pdf")

    pdf.cover_page(doc_title, subtitle)
    pdf.add_page()
    pdf.parse_markdown(text)

    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    pdf.output(str(pdf_path))
    size_kb = pdf_path.stat().st_size // 1024
    print(f"  Written  : {pdf_path.name}  ({size_kb} KB, {pdf.page_no()} pages)")


def main():
    base = pathlib.Path(__file__).parent.parent
    docs = base / "docs"

    jobs = [
        (
            base / "README.md",
            docs / "README.pdf",
            "DSR|RIECT AI",
            "Retail Intelligence Execution Control Tower — User Guide",
        ),
        (
            base / "ARCHITECTURE.md",
            docs / "ARCHITECTURE.pdf",
            "DSR|RIECT Architecture",
            "Technical Architecture & System Design Reference",
        ),
    ]

    ok = True
    for md_path, pdf_path, title, sub in jobs:
        if not md_path.exists():
            print(f"  SKIP : {md_path} not found")
            continue
        try:
            convert(md_path, pdf_path, title, sub)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            print(f"  ERROR: {exc}", file=sys.stderr)
            ok = False

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
