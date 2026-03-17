# core/guidelines_parser.py

import copy
import os
import re
from core.notion_client import NotionClient
from core.logger import get_logger
from dotenv import load_dotenv


load_dotenv()


GUIDELINES_DB_ID = os.getenv("NOTION_GUIDELINES_DB_ID")


class GuidelinesParser:
    def __init__(self, notion_client: NotionClient):
        self.logger = get_logger("GuidelinesParser")
        self.notion = notion_client
        self.guidelines_ds_id = None


    def _init_datasource(self):
        if not self.guidelines_ds_id and GUIDELINES_DB_ID:
            self.guidelines_ds_id = self.notion.get_data_source_id(GUIDELINES_DB_ID) or GUIDELINES_DB_ID


    def find_guidelines_document(self, process_type_string):
        """Searches for the Interview Stages document."""
        self._init_datasource()
        if not self.guidelines_ds_id: return None

        parts = process_type_string.split(" - ")
        if len(parts) < 2:
            self.logger.debug(f"find_guidelines_document: cannot parse process type '{process_type_string}' (no ' - ' separator)")
            return None

        prefix = parts[0].strip()
        role = parts[1].strip()
        self.logger.debug(f"find_guidelines_document: prefix='{prefix}', role='{role}'")

        filters = []
        if prefix == "PortCo":
            filters = [
                {"property": "Company", "select": {"equals": "PortCo"}},
                {"property": "Role", "select": {"equals": role}}
            ]
        else:
            filters = [
                {"property": "Company", "select": {"equals": "Nzyme"}},
                {"property": "Team", "select": {"equals": prefix}},
                {"property": "Role", "select": {"equals": role}}
            ]


        final_filter = {
            "and": [
                {"property": "Document Type", "select": {"equals": "Interview Stages"}}
            ] + filters
        }

        self.logger.debug(f"find_guidelines_document: querying with filter Document Type=Interview Stages, prefix={prefix}, role={role}")
        results = self.notion.query_data_source(self.guidelines_ds_id, final_filter)
        self.logger.debug(f"find_guidelines_document: {len(results)} result(s) returned")
        return results[0] if results else None


    def find_job_description_document(self, process_type_string):
        """Searches for the Job Description document."""
        self._init_datasource()
        if not self.guidelines_ds_id: return None

        parts = process_type_string.split(" - ")
        if len(parts) < 2:
            self.logger.debug(f"find_job_description_document: cannot parse process type '{process_type_string}' (no ' - ' separator)")
            return None

        prefix = parts[0].strip()
        role = parts[1].strip()
        self.logger.debug(f"find_job_description_document: prefix='{prefix}', role='{role}'")

        filters = []
        if prefix == "PortCo":
            filters = [
                {"property": "Company", "select": {"equals": "PortCo"}},
                {"property": "Role", "select": {"equals": role}}
            ]
        else:
            filters = [
                {"property": "Company", "select": {"equals": "Nzyme"}},
                {"property": "Team", "select": {"equals": prefix}},
                {"property": "Role", "select": {"equals": role}}
            ]


        final_filter = {
            "and": [
                {"property": "Document Type", "select": {"equals": "Job Description"}}
            ] + filters
        }

        self.logger.debug(f"find_job_description_document: querying with filter Document Type=Job Description, prefix={prefix}, role={role}")
        results = self.notion.query_data_source(self.guidelines_ds_id, final_filter)
        self.logger.debug(f"find_job_description_document: {len(results)} result(s) returned")
        return results[0] if results else None


    def extract_assessment_characteristics(self, guideline_page_id):
        """Extracts assessment characteristics from 'Assessment Characteristics' child DB inside a guideline page.
        Returns list of {"characteristic": str, "definition": str} or None if DB not found."""
        child_db_id = self.notion.find_child_database(guideline_page_id, "Assessment Characteristics")
        if not child_db_id:
            self.logger.debug(f"extract_assessment_characteristics: no 'Assessment Characteristics' DB in page {guideline_page_id[:8]}...")
            return None

        ds_id = self.notion.get_data_source_id(child_db_id) or child_db_id
        rows = self.notion.query_data_source(ds_id, filter_params=None)

        result = []
        for row in rows:
            props = row.get("properties", {})
            char_title = props.get("Characteristic", {}).get("title", [])
            characteristic = char_title[0]["plain_text"].strip() if char_title else ""
            def_text = props.get("Definition", {}).get("rich_text", [])
            definition = def_text[0]["plain_text"].strip() if def_text else ""
            if characteristic:
                result.append({"characteristic": characteristic, "definition": definition})

        if result:
            self.logger.debug(f"extract_assessment_characteristics: {len(result)} item(s) extracted")
        else:
            self.logger.debug("extract_assessment_characteristics: DB found but no rows")
        return result if result else None


    def extract_page_content(self, page_id):
        """
        Recursively downloads page content to clone it.
        ROBUST version: Handles Tables, Lists, Columns and avoids 400 validation errors.
        """
        root_blocks = self.notion.get_page_blocks(page_id)
        return self._process_block_list(root_blocks)



    def _sanitize_block(self, block):
        """
        Remove read-only properties that Notion API rejects on create.
        Preserves: object, type, and the type-specific data (with colors).
        """
        PROPS_READONLY = {
            "id", "created_by", "created_time", "last_edited_by",
            "last_edited_time", "parent", "archived", "in_trash",
            "request_id", "has_children"
        }
        return {k: v for k, v in block.items() if k not in PROPS_READONLY}


    def _sanitize_rich_text(self, rich_text_obj):
        """
        Remove read-only properties from a rich text object.
        Keeps: type, text, annotations, mention, equation
        Removes: plain_text, href (read-only per Notion API docs)
        """
        PROPS_READONLY_RT = {"plain_text", "href"}
        return {k: v for k, v in rich_text_obj.items() if k not in PROPS_READONLY_RT}


    def _process_block_list(self, block_list):
        """
        Recursive helper that clones blocks while cleaning read-only properties.
        """
        result = []

        for b in block_list:
            block_type = b["type"]

            # 1. Ignore unsupported blocks
            if block_type in ["child_page", "child_database", "link_to_page", "unsupported"]:
                self.logger.debug(f"_process_block_list: skipping unsupported block type '{block_type}'")
                continue

            # 2. Get block data
            block_data = b.get(block_type, {})

            # 3. Build the new base block
            new_block = {
                "object": "block",
                "type": block_type,
                block_type: copy.deepcopy(block_data) if isinstance(block_data, dict) else {}
            }

            # 3.5 Sanitize: remove read-only properties from type data
            if isinstance(new_block[block_type], dict):
                new_block[block_type] = self._sanitize_block(new_block[block_type])

            # 3.6 For table_row: sanitize each rich text object in cells
            if block_type == "table_row" and "cells" in new_block[block_type]:
                new_block[block_type]["cells"] = [
                    [self._sanitize_rich_text(rt) for rt in cell]
                    for cell in new_block[block_type]["cells"]
                ]

            # 4. PROCESS CHILDREN RECURSIVELY
            if b.get("has_children"):
                self.logger.debug(f"_process_block_list: block '{block_type}' (id={b['id'][:8]}...) has children — recursing")
                try:
                    children = self.notion.get_page_blocks(b["id"])
                    processed_children = self._process_block_list(children)

                    if processed_children:
                        # Blocks that accept children INSIDE their type property
                        internal_children_blocks = [
                            "paragraph", "bulleted_list_item", "numbered_list_item",
                            "toggle", "to_do", "quote", "callout", "column",
                            "table", "template", "synced_block"
                        ]

                        if block_type in internal_children_blocks:
                            if not isinstance(new_block[block_type], dict):
                                new_block[block_type] = {}
                            new_block[block_type]["children"] = processed_children
                            self.logger.debug(f"_process_block_list: attached {len(processed_children)} children internally to '{block_type}'")

                        # Blocks that accept children at root (mainly column_list)
                        elif block_type == "column_list":
                            new_block["children"] = processed_children
                            self.logger.debug(f"_process_block_list: attached {len(processed_children)} children at root for 'column_list'")

                        # For any other type with children, try internal first
                        else:
                            if not isinstance(new_block[block_type], dict):
                                new_block[block_type] = {}
                            new_block[block_type]["children"] = processed_children
                            self.logger.debug(f"_process_block_list: attached {len(processed_children)} children internally (fallback) for '{block_type}'")

                except Exception as e:
                    print(f"[WARN] Could not get children of {b['id']}: {e}")

            # 5. Final validations
            # Column_list without children is not valid
            if block_type == "column_list" and not new_block.get("children"):
                continue

            # Empty Column is not valid
            if block_type == "column":
                if not new_block[block_type].get("children"):
                    continue

            # Synced_block requires special configuration, better to omit
            if block_type == "synced_block":
                continue

            result.append(new_block)

        return result



    # --- STAGE PARSING METHODS ---

    def _clean_cell(self, text):
        if not text: return ""
        return text.strip().replace("\n", " ")


    def _determine_color(self, stage_num):
        if not stage_num: return "default"
        match = re.match(r"(\d+)", stage_num)
        if not match: return "default"
        num = int(match.group(1))
        colors = {0: "gray", 1: "brown", 2: "orange", 3: "purple", 4: "blue", 5: "pink", 6: "yellow", 7: "red"}
        return colors.get(num, "default")


    def parse_stages_from_page(self, page_id):
        print(f"   [PARSER] Analyzing page {page_id}...")
        blocks = self.notion.get_page_blocks(page_id)
        self.logger.debug(f"parse_stages_from_page: fetched {len(blocks)} top-level blocks from page {page_id[:8]}...")

        dynamic_stages = []
        used_names = set()
        last_stage_number = "0"

        # Buffer to save decisions and release them at the end of the round
        pending_decisions = []


        # 1. Define the Fixed ones (Closures)
        fixed_stages = [
            {"name": "Offer", "color": "green"},
            {"name": "Hired", "color": "blue"},
            {"name": "Discarded completely for Nzyme", "color": "yellow"},
            {"name": "Disqualified only for this role", "color": "red"},
            {"name": "Lost for this process", "color": "gray"}
        ]
        fixed_names = {s["name"] for s in fixed_stages}


        # 2. Read Table (Dynamic)
        for block in blocks:
            if block["type"] == "table":
                rows = self.notion.get_page_blocks(block["id"])
                self.logger.debug(f"parse_stages_from_page: table block {block['id'][:8]}... has {len(rows)} rows")
                for row in rows:
                    if row["type"] != "table_row": continue
                    cells = row["table_row"].get("cells", [])
                    if len(cells) < 2: continue

                    col_num = self._clean_cell("".join([t["plain_text"] for t in cells[0]]))
                    col_name = self._clean_cell("".join([t["plain_text"] for t in cells[1]]))

                    # --- EXCLUSION FILTERS ---
                    if "Interview Type" in col_name or "#" in col_num: continue
                    if "Individual" in col_name: continue


                    final_name = None
                    color = "default"
                    is_decision = False
                    current_round_number = last_stage_number


                    if col_num and col_num[0].isdigit():
                        last_stage_number = col_num.split('.')[0]
                        current_round_number = last_stage_number
                        final_name = f"{col_num} {col_name}"
                        color = self._determine_color(col_num)

                    elif "Group Decision" in col_name or "Round" in col_name:
                        final_name = f"{last_stage_number} [ROUND DECISION]"
                        color = self._determine_color(last_stage_number)
                        is_decision = True
                        current_round_number = last_stage_number
                        self.logger.debug(f"parse_stages_from_page: decision row detected for round {last_stage_number} — '{final_name}'")


                    if final_name and final_name not in used_names and final_name not in fixed_names:
                        stage_obj = {"name": final_name[:100], "color": color}
                        used_names.add(final_name)

                        if is_decision:
                            pending_decisions.append({'round': current_round_number, 'stage': stage_obj})
                        else:
                            dynamic_stages.append(stage_obj)



        self.logger.debug(f"parse_stages_from_page: {len(dynamic_stages)} dynamic stages, {len(pending_decisions)} pending decisions extracted")

        # 3. Smart Decision Insertion (Post-Processing)
        list_with_decisions = []

        # Group by round to facilitate insertion at the end
        rounds = {}
        round_order = []

        for st in dynamic_stages:

            # Extract the round number from the name (e.g.: "1.1 Interview" -> "1")
            match = re.match(r"(\d+)", st["name"])
            if match:
                r_num = match.group(1)
                if r_num not in rounds:
                    rounds[r_num] = []
                    round_order.append(r_num)
                rounds[r_num].append(st)
            else:
                if "0" not in rounds:
                    rounds["0"] = []
                    round_order.append("0")
                rounds["0"].append(st)


        # Rebuild the list
        for r_num in round_order:
            list_with_decisions.extend(rounds[r_num])
            for dec in pending_decisions:
                if dec['round'] == r_num:
                    list_with_decisions.append(dec['stage'])
                    self.logger.debug(f"parse_stages_from_page: inserted decision for round {r_num} after {len(rounds[r_num])} stage(s)")
                    break  # Only 1 per round

        # 4. Final Construction
        final_list = list_with_decisions + fixed_stages
        self.logger.debug(f"parse_stages_from_page: final stage list has {len(final_list)} entries ({len(list_with_decisions)} dynamic + {len(fixed_stages)} fixed)")
        return final_list
