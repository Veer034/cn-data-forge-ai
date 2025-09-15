# document_section_utils.py
import re
from typing import List, Dict, Any, Optional, Tuple
from language_constants import LanguageConstants
from text_processing_utils import TextProcessingUtils

class DocumentSectionUtils:
    """Utilities for identifying and processing document sections"""
    
    @staticmethod
    def identify_document_sections(text: str, language: str, 
                                   overlap_sentences: int = 2, 
                                   min_section_length: int = 100,
                                   max_section_length: int = 2000,
                                   min_divider_length: int = 5) -> List[Tuple[str, str]]:
        """
        Identify document sections with manual dividers as priority, then language-specific patterns and overlapping content.
        
        Args:
            text: Document text
            language: Language code
            overlap_sentences: Number of sentences to overlap between sections
            min_section_length: Minimum character length for a section
            max_section_length: Maximum character length before splitting
            min_divider_length: Minimum length for manual dividers (default: 5)
            
        Returns:
            List of (section_title, section_content) tuples with overlapping content
        """
        if not text.strip():
            return [("General", text)]
        
        script_group = TextProcessingUtils.get_script_group(language, LanguageConstants.SCRIPT_GROUPS)
        
        # PRIORITY 1: Check for manual dividers first
        manual_sections = DocumentSectionUtils._identify_manual_dividers(text, min_divider_length)
        if manual_sections:
            print(f"Found {len(manual_sections)} sections using manual dividers")
            
            # Check if any manual sections are too large and need further subdivision
            final_manual_sections = []
            for title, content in manual_sections:
                if len(content) > max_section_length:
                    print(f"Section '{title}' is too large ({len(content)} chars), subdividing...")
                    # Subdivide large manual sections
                    sub_sections = DocumentSectionUtils._subdivide_large_section(
                        title, content, language, script_group, 
                        max_section_length, min_section_length
                    )
                    final_manual_sections.extend(sub_sections)
                else:
                    final_manual_sections.append((title, content))
            
            # Add overlapping content between manually divided sections
            overlapped_sections = DocumentSectionUtils._add_section_overlaps(
                final_manual_sections, text, language, overlap_sentences
            )
            return DocumentSectionUtils._ensure_complete_coverage(overlapped_sections, text, language)
        
        # PRIORITY 2: Try to identify natural sections using existing logic
        sections = DocumentSectionUtils._identify_natural_sections(text, language, script_group)
        
        # PRIORITY 3: If no natural sections found or sections are too long, create artificial sections
        if not sections or any(len(content) > max_section_length for _, content in sections):
            sections = DocumentSectionUtils._create_chunked_sections(
                text, language, script_group, max_section_length, min_section_length
            )
        
        # Add overlapping content between sections
        overlapped_sections = DocumentSectionUtils._add_section_overlaps(
            sections, text, language, overlap_sentences
        )
        
        # Ensure complete text coverage
        final_sections = DocumentSectionUtils._ensure_complete_coverage(overlapped_sections, text, language)
        
        return final_sections

    @staticmethod
    def _identify_manual_dividers(text: str, min_divider_length: int = 5) -> List[Tuple[str, str]]:
        """
        Identify sections separated by manual dividers like ======= (5+ characters).
        
        Args:
            text: Document text
            min_divider_length: Minimum length for dividers
            
        Returns:
            List of (section_title, section_content) tuples or empty list if no dividers found
        """
        # Define various divider patterns that users might use
        divider_patterns = [
            rf'(?:^|\n)\s*([=]{{{min_divider_length},}})\s*(?:\n|$)',  # ======= (primary)
            rf'(?:^|\n)\s*([-]{{{min_divider_length},}})\s*(?:\n|$)',  # ------- 
            rf'(?:^|\n)\s*([*]{{{min_divider_length},}})\s*(?:\n|$)',  # *******
            rf'(?:^|\n)\s*([#]{{{min_divider_length},}})\s*(?:\n|$)',  # #######
            rf'(?:^|\n)\s*([~]{{{min_divider_length},}})\s*(?:\n|$)',  # ~~~~~~~
            rf'(?:^|\n)\s*([_]{{{min_divider_length},}})\s*(?:\n|$)',  # _______
            rf'(?:^|\n)\s*([+]{{{min_divider_length},}})\s*(?:\n|$)',  # +++++++
        ]
        
        for pattern in divider_patterns:
            divider_matches = list(re.finditer(pattern, text, re.MULTILINE))
            
            if len(divider_matches) >= 1:  # At least one divider found
                print(f"Found {len(divider_matches)} manual dividers with pattern: {pattern}")
                
                sections = []
                start_pos = 0
                section_num = 1
                
                for i, match in enumerate(divider_matches):
                    divider_start = match.start()
                    divider_end = match.end()
                    
                    # Extract content before this divider
                    if divider_start > start_pos:
                        content_before = text[start_pos:divider_start].strip()
                        if content_before and len(content_before) >= 10:  # Minimum content check
                            title = TextProcessingUtils.extract_section_title_from_content(content_before, section_num)
                            sections.append((title, content_before))
                            section_num += 1
                    
                    # Update start position for next section
                    start_pos = divider_end
                
                # Handle content after the last divider
                if start_pos < len(text):
                    remaining_content = text[start_pos:].strip()
                    if remaining_content and len(remaining_content) >= 10:
                        title = TextProcessingUtils.extract_section_title_from_content(remaining_content, section_num)
                        sections.append((title, remaining_content))
                
                # If we found meaningful sections, return them
                if sections and len(sections) > 1:
                    return sections
                elif sections and len(sections) == 1:
                    # Single section with dividers - might be title/content separation
                    return sections
        
        # Also check for combination dividers (text between dividers)
        combined_pattern = rf'(?:^|\n)\s*[=\-*#~_+]{{{min_divider_length},}}\s*(?:\n|$)'
        all_dividers = list(re.finditer(combined_pattern, text, re.MULTILINE))
        
        if len(all_dividers) >= 1:
            sections = []
            
            # Content before first divider
            if all_dividers[0].start() > 0:
                content = text[:all_dividers[0].start()].strip()
                if content and len(content) >= 10:
                    title = TextProcessingUtils.extract_section_title_from_content(content, 1)
                    sections.append((title, content))
            
            # Content between dividers
            for i in range(len(all_dividers)):
                start_pos = all_dividers[i].end()
                end_pos = all_dividers[i + 1].start() if i + 1 < len(all_dividers) else len(text)
                
                content = text[start_pos:end_pos].strip()
                if content and len(content) >= 10:
                    title = TextProcessingUtils.extract_section_title_from_content(content, len(sections) + 1)
                    sections.append((title, content))
            
            if len(sections) > 0:
                return sections
        
        return []  # No manual dividers found

    @staticmethod
    def _identify_natural_sections(text: str, language: str, script_group: str) -> List[Tuple[str, str]]:
        """Identify natural document sections using various patterns."""
        
        # Try Markdown-style headers first (universal format)
        markdown_patterns = [
            # Level 1-4 headers with content
            (r'(?:^|\n)(#{1,4})\s+(.*?)(?:\n|$)((?:.|\n)*?)(?=\n#{1,4}|\Z)', 'markdown'),
            # Alternative markdown with underlines
            (r'(?:^|\n)(.+?)\n([=\-]{3,})\n((?:.|\n)*?)(?=\n.+?\n[=\-]{3,}|\Z)', 'underline')
        ]
        
        for pattern, pattern_type in markdown_patterns:
            matches = re.finditer(pattern, text, re.MULTILINE | re.DOTALL)
            sections = []
            for m in matches:
                if pattern_type == 'markdown':
                    title = m.group(2).strip()
                    content = m.group(3).strip()
                else:  # underline
                    title = m.group(1).strip()
                    content = m.group(3).strip()
                
                if content:  # Only add sections with actual content
                    sections.append((title, content))
            
            if sections:
                return sections
        
        # Try language-specific section headers
        section_terms = LanguageConstants.SECTION_HEADERS.get(language, LanguageConstants.SECTION_HEADERS['en'])
        sections = DocumentSectionUtils._find_sections_by_headers(text, section_terms, script_group)
        
        if sections:
            return sections
        
        # Try numbered sections
        numbered_sections = DocumentSectionUtils._find_numbered_sections(text, script_group)
        if numbered_sections:
            return numbered_sections
        
        # Try other visual separators (but not the manual dividers we already checked)
        separator_sections = DocumentSectionUtils._find_other_separator_sections(text)
        if separator_sections:
            return separator_sections
        
        return []

    @staticmethod
    def _find_sections_by_headers(text: str, section_terms: List[str], script_group: str) -> List[Tuple[str, str]]:
        """Find sections using language-specific headers."""
        if not section_terms:
            return []
        
        section_patterns = []
        for term in section_terms:
            if script_group in ['latin', 'cyrillic', 'greek']:
                # Case-insensitive matching for Latin-based scripts
                pattern = rf'(?:^|\n)(?:{re.escape(term)}|{re.escape(term.capitalize())}|{re.escape(term.upper())})\s*\d*[\.\:：]\s*([^\n]*?)(?:\n|$)'
            else:
                # For other scripts
                pattern = rf'(?:^|\n){re.escape(term)}\s*\d*[\.\:：。]\s*([^\n]*?)(?:\n|$)'
            section_patterns.append((pattern, term))
        
        all_headers = []
        for pattern, term in section_patterns:
            for match in re.finditer(pattern, text, re.MULTILINE | re.IGNORECASE):
                start_pos = match.start()
                header_text = match.group(1).strip() if match.group(1).strip() else term
                all_headers.append((start_pos, header_text, match.end()))
        
        if not all_headers:
            return []
        
        # Sort headers by position
        all_headers.sort(key=lambda x: x[0])
        
        sections = []
        for i, (start_pos, header, header_end) in enumerate(all_headers):
            # Find content until next header or end of text
            next_pos = all_headers[i + 1][0] if i + 1 < len(all_headers) else len(text)
            content = text[header_end:next_pos].strip()
            
            if content:
                sections.append((header, content))
        
        return sections

    @staticmethod
    def _find_numbered_sections(text: str, script_group: str) -> List[Tuple[str, str]]:
        """Find numbered sections based on script group."""
        if script_group == 'cjk':
            # CJK numbering patterns
            patterns = [
                r'(?:^|\n)([一二三四五六七八九十百千]+[\.．、])\s*([^\n]*?)(?:\n|$)',
                r'(?:^|\n)([0-9]+[\.．、])\s*([^\n]*?)(?:\n|$)',
                r'(?:^|\n)([第][一二三四五六七八九十百千]+[章节部分])\s*([^\n]*?)(?:\n|$)'
            ]
        elif script_group in ['devanagari', 'bengali', 'dravidian', 'gurmukhi', 'gujarati']:
            # Indic script numbering
            patterns = [
                r'(?:^|\n)([०१२३४५६७८९]+[\.।])\s*([^\n]*?)(?:\n|$)',
                r'(?:^|\n)(\d+[\.।])\s*([^\n]*?)(?:\n|$)'
            ]
        elif script_group == 'arabic':
            # Arabic script numbering
            patterns = [
                r'(?:^|\n)([١٢٣٤٥٦٧٨٩٠]+[\.،])\s*([^\n]*?)(?:\n|$)',
                r'(?:^|\n)(\d+[\.،])\s*([^\n]*?)(?:\n|$)'
            ]
        elif script_group == 'thai':
            # Thai numbering
            patterns = [
                r'(?:^|\n)([๐๑๒๓๔๕๖๗๘๙]+[\.])\s*([^\n]*?)(?:\n|$)',
                r'(?:^|\n)(\d+[\.])?\s*([^\n]*?)(?:\n|$)'
            ]
        else:
            # Default Latin numbering
            patterns = [
                r'(?:^|\n)(\d+[\.]\s*[^\n]*?)(?:\n|$)',
                r'(?:^|\n)([a-zA-Z]\.\s*[^\n]*?)(?:\n|$)',
                r'(?:^|\n)([IVX]+\.\s*[^\n]*?)(?:\n|$)'
            ]
        
        for pattern in patterns:
            headers = list(re.finditer(pattern, text, re.MULTILINE))
            if len(headers) >= 2:  # Need at least 2 sections
                sections = []
                for i, match in enumerate(headers):
                    start_pos = match.start()
                    if len(match.groups()) >= 2:
                        number = match.group(1).strip()
                        title = match.group(2).strip() if match.group(2) else f"Section {number}"
                    else:
                        title = f"Section {i + 1}"
                        content = part
                    
                    if content:
                        sections.append((title, content))
                
                if len(sections) > 1:
                    return sections
        
        return []

    @staticmethod
    def _create_chunked_sections(text: str, language: str, script_group: str,
                                max_section_length: int, min_section_length: int) -> List[Tuple[str, str]]:
        """Create artificial sections by chunking the text intelligently."""
        if len(text) <= max_section_length:
            return [("General", text)]
        
        # Get sentence boundaries based on script group
        sentence_endings = DocumentSectionUtils._get_sentence_endings(script_group)
        
        sections = []
        current_pos = 0
        section_num = 1
        
        while current_pos < len(text):
            # Find a good breaking point within max_section_length
            end_pos = min(current_pos + max_section_length, len(text))
            
            # First, check if we're in the middle of a list - if so, find the end
            best_break = DocumentSectionUtils._find_safe_break_point(
                text, current_pos, end_pos, min_section_length, sentence_endings, script_group
            )
            
            # If we still don't have a good break and we're not at the end, extend to complete the list
            if best_break == end_pos and end_pos < len(text):
                list_end = DocumentSectionUtils._find_list_end(text, end_pos, script_group)
                if list_end > end_pos and list_end - current_pos < max_section_length * 1.5:  # Allow 50% extension for lists
                    best_break = list_end
            
            section_content = text[current_pos:best_break].strip()
            if section_content:
                # Generate meaningful title from first line or sentences
                title = TextProcessingUtils.extract_section_title_from_content(section_content, section_num)
                sections.append((title, section_content))
                section_num += 1
            
            current_pos = best_break
        
        return sections

    @staticmethod
    def _subdivide_large_section(parent_title: str, content: str, language: str, script_group: str,
                                max_section_length: int, min_section_length: int) -> List[Tuple[str, str]]:
        """
        Subdivide a large section (from manual dividers) into smaller manageable sections.
        
        Args:
            parent_title: Title of the parent section
            content: Content to subdivide
            language: Language code
            script_group: Script group for the language
            max_section_length: Maximum section length
            min_section_length: Minimum section length
            
        Returns:
            List of (title, content) tuples for subdivided sections
        """
        if len(content) <= max_section_length:
            return [(parent_title, content)]
        
        print(f"Subdividing large section: {parent_title}")
        
        # Try to find natural sub-sections within this large section first
        natural_subsections = DocumentSectionUtils._find_natural_subsections_in_content(content, language, script_group)
        
        if natural_subsections and len(natural_subsections) > 1:
            # Check if natural subsections are reasonable sized
            reasonable_subsections = []
            for sub_title, sub_content in natural_subsections:
                if len(sub_content) > max_section_length:
                    # Even natural subsections are too big, chunk them further
                    chunked = DocumentSectionUtils._create_chunked_sections_with_prefix(
                        sub_content, f"{parent_title} - {sub_title}", language, script_group,
                        max_section_length, min_section_length
                    )
                    reasonable_subsections.extend(chunked)
                else:
                    # Prefix with parent title to maintain hierarchy
                    full_title = f"{parent_title} - {sub_title}" if sub_title != parent_title else sub_title
                    reasonable_subsections.append((full_title, sub_content))
            
            if reasonable_subsections:
                return reasonable_subsections
        
        # No natural subsections found or they didn't work out, use intelligent chunking
        return DocumentSectionUtils._create_chunked_sections_with_prefix(
            content, parent_title, language, script_group, max_section_length, min_section_length
        )

    @staticmethod
    def _find_natural_subsections_in_content(content: str, language: str, script_group: str) -> List[Tuple[str, str]]:
        """
        Try to find natural subsections within a large content block.
        
        Args:
            content: Content to analyze
            language: Language code  
            script_group: Script group
            
        Returns:
            List of natural subsections found
        """
        # Try markdown headers (but lower level than main document)
        markdown_patterns = [
            # Level 2-6 headers (assuming main doc used level 1)
            (r'(?:^|\n)(#{2,6})\s+(.*?)(?:\n|$)((?:.|\n)*?)(?=\n#{2,6}|\Z)', 'markdown'),
            # Bold text headers **Header**
            (r'(?:^|\n)\*\*(.*?)\*\*\s*(?:\n|$)((?:.|\n)*?)(?=\n\*\*.*?\*\*|\Z)', 'bold'),
            # Underlined headers
            (r'(?:^|\n)(.+?)\n([-=]{2,})\n((?:.|\n)*?)(?=\n.+?\n[-=]{2,}|\Z)', 'underline')
        ]
        
        for pattern, pattern_type in markdown_patterns:
            matches = re.finditer(pattern, content, re.MULTILINE | re.DOTALL)
            sections = []
            for m in matches:
                if pattern_type == 'markdown':
                    title = m.group(2).strip()
                    section_content = m.group(3).strip()
                elif pattern_type == 'bold':
                    title = m.group(1).strip()
                    section_content = m.group(2).strip()
                else:  # underline
                    title = m.group(1).strip()
                    section_content = m.group(3).strip()
                
                if section_content and len(section_content) > 50:  # Reasonable content
                    sections.append((title, section_content))
            
            if len(sections) > 1:  # Need multiple sections to be useful
                return sections
        
        # Try numbered subsections
        numbered_patterns = DocumentSectionUtils._get_subsection_number_patterns(script_group)
        for pattern in numbered_patterns:
            headers = list(re.finditer(pattern, content, re.MULTILINE))
            if len(headers) >= 2:
                sections = []
                for i, match in enumerate(headers):
                    title = match.group(1).strip() if match.groups() else f"Subsection {i+1}"
                    
                    # Find content
                    content_start = match.end()
                    content_end = headers[i + 1].start() if i + 1 < len(headers) else len(content)
                    section_content = content[content_start:content_end].strip()
                    
                    if section_content and len(section_content) > 50:
                        sections.append((title, section_content))
                
                if len(sections) > 1:
                    return sections
        
        # Try paragraph-based separation for very large blocks
        paragraphs = re.split(r'\n\s*\n', content)
        if len(paragraphs) > 3:
            # Group paragraphs into subsections
            sections = []
            current_section = []
            current_length = 0
            target_length = len(content) // min(6, len(paragraphs))  # Aim for ~6 subsections max
            
            for i, paragraph in enumerate(paragraphs):
                paragraph = paragraph.strip()
                if not paragraph:
                    continue
                    
                current_section.append(paragraph)
                current_length += len(paragraph)
                
                # Create section if we've hit target length or it's the last paragraph
                if current_length >= target_length or i == len(paragraphs) - 1:
                    if current_section:
                        section_content = '\n\n'.join(current_section)
                        title = TextProcessingUtils.extract_section_title_from_content(section_content, len(sections) + 1)
                        sections.append((title, section_content))
                        current_section = []
                        current_length = 0
            
            if len(sections) > 1:
                return sections
        
        return []

    @staticmethod
    def _get_subsection_number_patterns(script_group: str) -> List[str]:
        """Get numbering patterns for subsections based on script group."""
        if script_group == 'cjk':
            return [
                r'(?:^|\n)([a-z]\)|[a-z]\.)\s*([^\n]*?)(?:\n|$)',  # a) or a.
                r'(?:^|\n)([0-9]+\.[0-9]+)\s*([^\n]*?)(?:\n|$)',   # 1.1, 1.2, etc.
                r'(?:^|\n)([(][0-9]+[)])\s*([^\n]*?)(?:\n|$)',     # (1), (2), etc.
            ]
        elif script_group in ['devanagari', 'bengali', 'dravidian', 'gurmukhi', 'gujarati']:
            return [
                r'(?:^|\n)([a-z]\)|[a-z]\.)\s*([^\n]*?)(?:\n|$)',
                r'(?:^|\n)([0-9]+\.[0-9]+)\s*([^\n]*?)(?:\n|$)',
                r'(?:^|\n)([(][0-9]+[)])\s*([^\n]*?)(?:\n|$)',
            ]
        elif script_group == 'arabic':
            return [
                r'(?:^|\n)([a-z]\)|[a-z]\.)\s*([^\n]*?)(?:\n|$)',
                r'(?:^|\n)([0-9]+\.[0-9]+)\s*([^\n]*?)(?:\n|$)',
                r'(?:^|\n)([(][0-9]+[)])\s*([^\n]*?)(?:\n|$)',
            ]
        else:
            return [
                r'(?:^|\n)([a-z]\)|[a-z]\.)\s*([^\n]*?)(?:\n|$)',  # a) or a.
                r'(?:^|\n)([0-9]+\.[0-9]+)\s*([^\n]*?)(?:\n|$)',   # 1.1, 1.2, etc.
                r'(?:^|\n)([(][0-9]+[)])\s*([^\n]*?)(?:\n|$)',     # (1), (2), etc.
                r'(?:^|\n)([i][v]*\.|[i][v]*\))\s*([^\n]*?)(?:\n|$)',  # i., ii., iii., etc.
            ]

    @staticmethod
    def _create_chunked_sections_with_prefix(text: str, parent_title: str, language: str, script_group: str,
                                            max_section_length: int, min_section_length: int) -> List[Tuple[str, str]]:
        """
        Create chunked sections with parent title prefix for large manual sections.
        
        Args:
            text: Text to chunk
            parent_title: Parent section title to prefix subsections
            language: Language code
            script_group: Script group
            max_section_length: Maximum section length
            min_section_length: Minimum section length
            
        Returns:
            List of chunked sections with prefixed titles
        """
        if len(text) <= max_section_length:
            return [(parent_title, text)]
        
        # Get sentence boundaries based on script group
        sentence_endings = DocumentSectionUtils._get_sentence_endings(script_group)
        
        sections = []
        current_pos = 0
        chunk_num = 1
        
        while current_pos < len(text):
            # Find a good breaking point within max_section_length
            end_pos = min(current_pos + max_section_length, len(text))
            
            # Try to break at sentence boundary
            best_break = end_pos
            for i in range(end_pos - 1, max(current_pos + min_section_length, end_pos - 200), -1):
                if text[i] in sentence_endings and i > current_pos + min_section_length:
                    best_break = i + 1
                    break
            
            # If no good sentence break found, try paragraph break
            if best_break == end_pos and end_pos < len(text):
                for i in range(end_pos - 1, max(current_pos + min_section_length, end_pos - 100), -1):
                    if text[i:i+2] == '\n\n':
                        best_break = i + 2
                        break
            
            section_content = text[current_pos:best_break].strip()
            if section_content:
                # Create hierarchical title
                if chunk_num == 1:
                    # First chunk keeps the original title
                    title = parent_title
                else:
                    # Subsequent chunks get numbered
                    part_word = LanguageConstants.PART_WORDS.get(language, 'Part')
                    title = f"{parent_title} - {part_word} {chunk_num}"
                
                sections.append((title, section_content))
                chunk_num += 1
            
            current_pos = best_break
        
        return sections

    @staticmethod
    def _find_safe_break_point(text: str, current_pos: int, end_pos: int, 
                              min_section_length: int, sentence_endings: str, script_group: str) -> int:
        """
        Find a safe break point that doesn't split ordered lists, bullet points, or numbered items.
        """
        # Check if we're in the middle of a list at end_pos
        if DocumentSectionUtils._is_in_list_context(text, end_pos):
            # Try to find the end of the current list
            list_end = DocumentSectionUtils._find_list_end(text, end_pos, script_group)
            # If list end is reasonable, use it
            if list_end <= end_pos + 500:  # Don't extend too far
                return list_end
        
        # Try to break at sentence boundary, but avoid breaking lists
        best_break = end_pos
        for i in range(end_pos - 1, max(current_pos + min_section_length, end_pos - 200), -1):
            if text[i] in sentence_endings and i > current_pos + min_section_length:
                # Check if this sentence ending is safe (not in middle of a list)
                if not DocumentSectionUtils._is_in_list_context(text, i):
                    best_break = i + 1
                    break
        
        # If no good sentence break found, try paragraph break
        if best_break == end_pos and end_pos < len(text):
            for i in range(end_pos - 1, max(current_pos + min_section_length, end_pos - 100), -1):
                if text[i:i+2] == '\n\n':
                    # Double check this isn't breaking a list
                    if not DocumentSectionUtils._is_in_list_context(text, i):
                        best_break = i + 2
                        break
        
        return best_break

    @staticmethod
    def _is_in_list_context(text: str, position: int) -> bool:
        """
        Check if the given position is within a list context (numbered, bulleted, etc.).
        """
        # Look backward to see if we're in a list
        lines_to_check = 5  # Check previous 5 lines
        
        # Find the line containing this position
        lines_before = text[:position].split('\n')
        if not lines_before:
            return False
        
        # Check current line and previous lines for list patterns
        start_line = max(0, len(lines_before) - lines_to_check)
        recent_lines = lines_before[start_line:]
        
        list_pattern_count = 0
        
        for line in recent_lines:
            line = line.strip()
            if TextProcessingUtils.is_list_item(line):
                list_pattern_count += 1
        
        # If we found multiple list items recently, we're likely in a list
        return list_pattern_count >= 2

    @staticmethod
    def _find_list_end(text: str, start_pos: int, script_group: str) -> int:
        """
        Find the end of a list starting from the given position.
        """
        lines = text[start_pos:].split('\n')
        
        list_end_pos = start_pos
        consecutive_non_list_lines = 0
        in_list = False
        
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            
            # Check if this line is a list item
            if TextProcessingUtils.is_list_item(line_stripped):
                in_list = True
                consecutive_non_list_lines = 0
                # Update end position to include this line
                list_end_pos = start_pos + len('\n'.join(lines[:i+1]))
            
            elif in_list:
                # We were in a list, check if this line continues the list context
                if DocumentSectionUtils._is_list_continuation(line_stripped):
                    # This line continues the previous list item (indented content, etc.)
                    consecutive_non_list_lines = 0
                    list_end_pos = start_pos + len('\n'.join(lines[:i+1]))
                
                elif line_stripped == '':
                    # Empty line - might be separating list items
                    consecutive_non_list_lines += 1
                    if consecutive_non_list_lines >= 2:
                        # Multiple empty lines likely end the list
                        break
                
                else:
                    # Non-list, non-empty line
                    consecutive_non_list_lines += 1
                    if consecutive_non_list_lines >= 1:
                        # Non-list content likely ends the list
                        break
            
            else:
                # We haven't found a list yet
                if line_stripped != '':
                    consecutive_non_list_lines += 1
                    if consecutive_non_list_lines >= 3:
                        # No list found in reasonable distance
                        break
        
        return min(list_end_pos + 1, len(text))  # Add 1 to include the newline

    @staticmethod
    def _is_list_continuation(line: str) -> bool:
        """
        Check if a line is a continuation of a list item (like indented content).
        """
        if not line.strip():
            return True  # Empty lines can be part of list formatting
        
        # Check for indented content (common in list continuations)
        if re.match(r'^\s{2,}[^\s\-\*\+•·]', line):
            return True
        
        # Check for content that looks like it continues a list item
        continuation_patterns = [
            r'^\s+\w+',                 # Indented text
            r'^\s*["\'"„"«»]',          # Quoted content (often in lists)
            r'^\s*[\(\[][^0-9a-zA-Z]',  # Parenthetical content (not new list items)
        ]
        
        for pattern in continuation_patterns:
            if re.match(pattern, line):
                return True
        
        return False

    @staticmethod
    def _get_sentence_endings(script_group: str) -> str:
        """Get sentence ending characters based on script group."""
        endings = {
            'latin': '.!?',
            'cyrillic': '.!?',
            'arabic': '.!?؟',
            'cjk': '。！？',
            'devanagari': '।!?',
            'bengali': '।!?',
            'dravidian': '।!?',
            'thai': '.!?',
            'greek': '.!?;',
            'hebrew': '.!?',
            'gurmukhi': '।!?',
            'gujarati': '।!?',
            'sinhala': '.!?',
            'african': '.!?'
        }
        return endings.get(script_group, '.!?')

    @staticmethod
    def _add_section_overlaps(sections: List[Tuple[str, str]], full_text: str,
                             language: str, overlap_sentences: int) -> List[Tuple[str, str]]:
        """Add overlapping content between adjacent sections."""
        if len(sections) <= 1 or overlap_sentences <= 0:
            return sections
        
        script_group = TextProcessingUtils.get_script_group(language, LanguageConstants.SCRIPT_GROUPS)
        sentence_endings = DocumentSectionUtils._get_sentence_endings(script_group)
        
        overlapped_sections = []
        
        for i, (title, content) in enumerate(sections):
            extended_content = content
            
            # Add overlap from previous section (suffix)
            if i > 0:
                prev_content = sections[i-1][1]
                prev_sentences = DocumentSectionUtils._split_sentences(prev_content, sentence_endings)
                if len(prev_sentences) >= overlap_sentences:
                    overlap_text = ' '.join(prev_sentences[-overlap_sentences:])
                    extended_content = overlap_text + '\n\n' + extended_content
            
            # Add overlap from next section (prefix)
            if i < len(sections) - 1:
                next_content = sections[i+1][1]
                next_sentences = DocumentSectionUtils._split_sentences(next_content, sentence_endings)
                if len(next_sentences) >= overlap_sentences:
                    overlap_text = ' '.join(next_sentences[:overlap_sentences])
                    extended_content = extended_content + '\n\n' + overlap_text
            
            overlapped_sections.append((title, extended_content))
        
        return overlapped_sections

    @staticmethod
    def _split_sentences(text: str, sentence_endings: str) -> List[str]:
        """Split text into sentences based on language-specific endings."""
        sentences = []
        current_sentence = ''
        
        for char in text:
            current_sentence += char
            if char in sentence_endings:
                # Check if this is actually end of sentence (not abbreviation)
                stripped = current_sentence.strip()
                if stripped:
                    sentences.append(stripped)
                    current_sentence = ''
        
        # Add remaining text as last sentence if any
        if current_sentence.strip():
            sentences.append(current_sentence.strip())
        
        return sentences

    @staticmethod
    def _ensure_complete_coverage(self, sections: List[Tuple[str, str]], 
                                full_text: str, language: str) -> List[Tuple[str, str]]:
        """Ensure all text is covered in sections, add missing parts if needed."""
        if not sections:
            return [("General", full_text)]
        
        # Create a simple coverage check by looking at unique phrases
        covered_text = ' '.join([content for _, content in sections])
        
        # If original text is much longer than covered text, we might be missing content
        if len(full_text) > len(covered_text) * 1.5:
            # Add the full text as a comprehensive section
            sections.append(("Complete Document", full_text))
        
        # Ensure we have at least one section with reasonable content
        if all(len(content.strip()) < 50 for _, content in sections):
            sections = [("General", full_text)]
        
        return sections



    @staticmethod
    def _find_other_separator_sections(self, text: str) -> List[Tuple[str, str]]:
        """Find sections separated by other visual separators (excluding manual dividers)."""
        separator_patterns = [
            r'\n\s*[•·]{3,}\s*\n',    # Bullet separators
            r'\n\s*\n\s*\n',          # Multiple blank lines
            r'\n\s*[~]{3,4}\s*\n'     # Short tilde separators (not manual dividers)
        ]
        
        for pattern in separator_patterns:
            parts = re.split(pattern, text)
            if len(parts) > 1:
                sections = []
                for i, part in enumerate(parts):
                    part = part.strip()
                    if not part:
                        continue
                    
                    # Try to extract title from first line
                    lines = part.split('\n', 1)
                    if len(lines) > 1 and len(lines[0].strip()) < 100:
                        title = lines[0].strip()
                        content = lines[1].strip()
                    else:
                        title = f"Section {i + 1}"
                        content = part
                    
                    if content:
                        sections.append((title, content))
                
                if len(sections) > 1:
                    return sections
        
        return []


