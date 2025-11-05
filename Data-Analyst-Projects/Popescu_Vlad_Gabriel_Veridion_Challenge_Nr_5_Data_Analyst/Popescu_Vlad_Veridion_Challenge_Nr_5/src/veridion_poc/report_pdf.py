"""Lightweight PDF reporting utilities based on ReportLab."""

from __future__ import annotations

from typing import Iterable, List, Sequence, Tuple

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib.utils import simpleSplit
from reportlab.pdfgen import canvas

TITLE_FONT = ("Helvetica-Bold", 16)
HEADING_FONT = ("Helvetica-Bold", 12)
BODY_FONT = ("Helvetica", 10)
LINE_HEIGHT = 0.45 * cm
HEADING_SPACING = 0.6 * cm
SECTION_GAP = 0.35 * cm
BULLET_INDENT = 0.6 * cm


def _normalize_sections(sections: Iterable[Tuple[str, str]]) -> List[Tuple[str, str]]:
    normalized: List[Tuple[str, str]] = []
    for heading, body in sections or []:
        heading = str(heading or "").strip()
        body = str(body or "").rstrip()
        if not heading:
            continue
        normalized.append((heading, body))
    return normalized


def _ensure_space(
    c: canvas.Canvas,
    y: float,
    *,
    margin: float,
    needed: float,
    page_height: float,
) -> float:
    if y - needed < margin:
        c.showPage()
        y = page_height - margin
    return y


def _draw_bullet_paragraph(
    c: canvas.Canvas,
    bullet_text: str,
    *,
    page_height: float,
    x: float,
    y: float,
    max_width: float,
    margin: float,
) -> float:
    if not bullet_text:
        return y

    bullet_lines = simpleSplit(bullet_text, BODY_FONT[0], BODY_FONT[1], max_width - BULLET_INDENT)
    if not bullet_lines:
        bullet_lines = [bullet_text]

    for idx, line in enumerate(bullet_lines):
        y = _ensure_space(c, y, margin=margin, needed=LINE_HEIGHT, page_height=page_height)
        c.setFont(BODY_FONT[0], BODY_FONT[1])
        if idx == 0:
            c.drawString(x, y, "•")
        c.drawString(x + BULLET_INDENT, y, line)
        y -= LINE_HEIGHT
    return y


def _draw_paragraph(
    c: canvas.Canvas,
    text: str,
    *,
    page_height: float,
    x: float,
    y: float,
    max_width: float,
    margin: float,
) -> float:
    if not text:
        return y - LINE_HEIGHT

    lines = simpleSplit(text, BODY_FONT[0], BODY_FONT[1], max_width)
    if not lines:
        lines = [""]

    for line in lines:
        y = _ensure_space(c, y, margin=margin, needed=LINE_HEIGHT, page_height=page_height)
        c.setFont(BODY_FONT[0], BODY_FONT[1])
        c.drawString(x, y, line)
        y -= LINE_HEIGHT
    return y


def _draw_body(
    c: canvas.Canvas,
    body: str,
    *,
    page_height: float,
    x: float,
    y: float,
    max_width: float,
    margin: float,
) -> float:
    if not body:
        return y - LINE_HEIGHT

    lines = body.splitlines() or [body]
    for raw in lines:
        stripped = raw.strip()
        if stripped.startswith("- "):
            y = _draw_bullet_paragraph(
                c,
                stripped[2:].strip(),
                page_height=page_height,
                x=x,
                y=y,
                max_width=max_width,
                margin=margin,
            )
        elif stripped:
            y = _draw_paragraph(
                c,
                stripped,
                page_height=page_height,
                x=x,
                y=y,
                max_width=max_width,
                margin=margin,
            )
        else:
            y -= LINE_HEIGHT
    return y


def generate_pdf(path: str, title: str, sections: Sequence[Tuple[str, str]]) -> None:
    """Create an A4 PDF with a title and structured sections."""
    sections = _normalize_sections(sections)
    c = canvas.Canvas(path, pagesize=A4)
    width, height = A4
    margin = 2 * cm
    usable_width = width - 2 * margin
    y = height - margin

    # Title
    c.setFont(*TITLE_FONT)
    y = _ensure_space(c, y, margin=margin, needed=LINE_HEIGHT, page_height=height)
    c.drawString(margin, y, title.strip() or "Raport Veridion")
    y -= HEADING_SPACING

    for heading, body in sections:
        y = _ensure_space(
            c,
            y,
            margin=margin,
            needed=HEADING_SPACING + LINE_HEIGHT,
            page_height=height,
        )
        c.setFont(*HEADING_FONT)
        c.drawString(margin, y, heading)
        y -= HEADING_SPACING

        y = _draw_body(
            c,
            body,
            page_height=height,
            x=margin,
            y=y,
            max_width=usable_width,
            margin=margin,
        )
        y -= SECTION_GAP

    c.save()
