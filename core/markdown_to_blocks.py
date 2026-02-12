"""
Converts a Markdown string into a list of Notion API block objects.

Supports: headings (#/##/###), bullets (-/*), numbered lists (1.),
dividers (---), **bold**, *italic*, and paragraphs.
Auto-splits rich_text segments at the 2000-char Notion limit.
"""

import re

NOTION_TEXT_LIMIT = 2000


def _split_text(text, limit=NOTION_TEXT_LIMIT):
    """Split text into chunks of at most `limit` characters."""
    chunks = []
    while len(text) > limit:
        # Try to break at last space before limit
        idx = text.rfind(" ", 0, limit)
        if idx == -1:
            idx = limit
        chunks.append(text[:idx])
        text = text[idx:].lstrip()
    if text:
        chunks.append(text)
    return chunks


def _parse_inline(text):
    """
    Parse inline markdown (**bold**, *italic*) into Notion rich_text objects.
    Returns a list of rich_text objects with appropriate annotations.
    """
    rich_text = []
    # Match **bold**, *italic*, or plain text segments
    pattern = re.compile(r"(\*\*(.+?)\*\*|\*(.+?)\*|([^*]+))")

    for match in pattern.finditer(text):
        if match.group(2):  # **bold**
            content = match.group(2)
            for chunk in _split_text(content):
                rich_text.append({
                    "type": "text",
                    "text": {"content": chunk},
                    "annotations": {"bold": True}
                })
        elif match.group(3):  # *italic*
            content = match.group(3)
            for chunk in _split_text(content):
                rich_text.append({
                    "type": "text",
                    "text": {"content": chunk},
                    "annotations": {"italic": True}
                })
        elif match.group(4):  # plain text
            content = match.group(4)
            for chunk in _split_text(content):
                rich_text.append({
                    "type": "text",
                    "text": {"content": chunk}
                })

    return rich_text if rich_text else [{"type": "text", "text": {"content": ""}}]


def _make_block(block_type, rich_text):
    """Create a Notion block object."""
    return {
        "object": "block",
        "type": block_type,
        block_type: {"rich_text": rich_text}
    }


def markdown_to_notion_blocks(markdown_text):
    """
    Convert a Markdown string to a list of Notion block objects.

    Supports:
        # / ## / ### -> heading_1 / heading_2 / heading_3
        - / * -> bulleted_list_item
        1. -> numbered_list_item
        --- -> divider
        **bold**, *italic* -> rich_text annotations
        Everything else -> paragraph
    """
    if not markdown_text:
        return []

    blocks = []
    lines = markdown_text.split("\n")

    for line in lines:
        stripped = line.strip()

        # Skip empty lines
        if not stripped:
            continue

        # Divider
        if re.match(r"^-{3,}$", stripped):
            blocks.append({"object": "block", "type": "divider", "divider": {}})
            continue

        # Headings
        heading_match = re.match(r"^(#{1,3})\s+(.+)$", stripped)
        if heading_match:
            level = len(heading_match.group(1))
            content = heading_match.group(2).strip()
            block_type = f"heading_{level}"
            blocks.append(_make_block(block_type, _parse_inline(content)))
            continue

        # Bulleted list
        if re.match(r"^[-*]\s+", stripped):
            content = re.sub(r"^[-*]\s+", "", stripped)
            blocks.append(_make_block("bulleted_list_item", _parse_inline(content)))
            continue

        # Numbered list
        numbered_match = re.match(r"^\d+\.\s+(.+)$", stripped)
        if numbered_match:
            content = numbered_match.group(1)
            blocks.append(_make_block("numbered_list_item", _parse_inline(content)))
            continue

        # Default: paragraph
        blocks.append(_make_block("paragraph", _parse_inline(stripped)))

    return blocks
