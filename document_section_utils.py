import re
from typing import List, Dict, Any, Optional, Tuple
from language_constants import LanguageConstants
from text_processing_utils import TextProcessingUtils

class DocumentSectionUtils:
    """Utilities for identifying and processing document sections with multilingual support"""
    
    @staticmethod
    def identify_document_sections(text: str, language: str = 'en', 
                                   max_section_length: int = 4000,
                                   min_section_length: int = 300,
                                   overlap_sentences: int = 1) -> List[Tuple[str, str]]:
        """
        Main entry point for document section identification with multilingual support.
        """
        if not text.strip():
            return [("General", text)]
        
        # Get script group for language-specific processing
        script_group = TextProcessingUtils.get_script_group(language, LanguageConstants.SCRIPT_GROUPS)
        
        # PRIORITY 1: Check for manual dividers first
        sections = DocumentSectionUtils._identify_manual_dividers(text, min_length=10)
        
        if sections:
            print(f"Found {len(sections)} sections using manual dividers")
            
            # Only subdivide if sections are really too large
            final_sections = []
            for title, content in sections:
                if len(content) > max_section_length:
                    print(f"Large section '{title}' ({len(content)} chars) - subdividing")
                    sub_sections = DocumentSectionUtils._subdivide_large_section(
                        title, content, language, script_group, max_section_length, min_section_length
                    )
                    final_sections.extend(sub_sections)
                else:
                    final_sections.append((title, content))
            
            # Add minimal overlap with language-aware sentence splitting
            overlapped_sections = DocumentSectionUtils._add_minimal_overlap(
                final_sections, language, script_group, overlap_sentences
            )
            return DocumentSectionUtils._ensure_complete_coverage(overlapped_sections, text, language)
        
        # PRIORITY 2: Try to identify natural sections
        sections = DocumentSectionUtils._identify_natural_sections(text, language, script_group)
        
        # PRIORITY 3: If no natural sections found or sections are too long, create artificial sections
        if not sections or any(len(content) > max_section_length for _, content in sections):
            sections = DocumentSectionUtils._create_chunked_sections(
                text, language, script_group, max_section_length, min_section_length
            )
        
        # Add overlapping content between sections
        overlapped_sections = DocumentSectionUtils._add_minimal_overlap(
            sections, language, script_group, overlap_sentences
        )
        
        # Ensure complete text coverage
        final_sections = DocumentSectionUtils._ensure_complete_coverage(overlapped_sections, text, language)
        
        return final_sections

    @staticmethod
    def _identify_manual_dividers(text: str, min_length: int = 10) -> List[Tuple[str, str]]:
        """
        Improved manual divider detection that works across all languages.
        """
        # Multiple patterns for different divider styles
        divider_patterns = [
            r'(?:^|\n)\s*[=]{10,}\s*(?:\n|$)',      # ============ (primary)
            r'(?:^|\n)\s*[-]{10,}\s*(?:\n|$)',      # ------------
            r'(?:^|\n)\s*[*]{10,}\s*(?:\n|$)',      # ************
            r'(?:^|\n)\s*[#]{5,}\s*(?:\n|$)',       # ############
            r'(?:^|\n)\s*[~]{10,}\s*(?:\n|$)',      # ~~~~~~~~~~~~
            r'(?:^|\n)\s*[_]{10,}\s*(?:\n|$)',      # ____________
        ]
        
        for pattern in divider_patterns:
            divider_matches = list(re.finditer(pattern, text, re.MULTILINE))
            
            if len(divider_matches) >= 1:
                print(f"Found {len(divider_matches)} section dividers")
                
                sections = []
                start_pos = 0
                
                for i, match in enumerate(divider_matches):
                    divider_start = match.start()
                    
                    # Get content before this divider
                    if divider_start > start_pos:
                        content = text[start_pos:divider_start].strip()
                        if content and len(content) >= min_length:
                            title = DocumentSectionUtils._extract_meaningful_title(content)
                            sections.append((title, content))
                    
                    # Update start position for next section
                    start_pos = match.end()
                
                # Handle content after the last divider
                if start_pos < len(text):
                    remaining_content = text[start_pos:].strip()
                    if remaining_content and len(remaining_content) >= min_length:
                        title = DocumentSectionUtils._extract_meaningful_title(remaining_content)
                        sections.append((title, remaining_content))
                
                if sections:
                    return sections
        
        return []

    @staticmethod
    def _extract_meaningful_title(content: str) -> str:
        """
        Extract meaningful title from content - works across all languages.
        """
        lines = content.split('\n')
        
        # Look for title patterns in first few lines
        for line in lines[:3]:
            line = line.strip()
            if not line:
                continue
                
            # Universal title patterns (work across scripts)
            if (len(line) < 100 and 
                (line.isupper() or 
                 DocumentSectionUtils._is_title_case_multilingual(line) or 
                 DocumentSectionUtils._contains_section_keywords_multilingual(line))):
                return line
        
        # Fallback: use first meaningful line
        for line in lines:
            line = line.strip()
            if line and len(line) > 10 and len(line) < 100:
                return line[:50] + ('...' if len(line) > 50 else '')
        
        return "Document Section"

    @staticmethod
    def _is_title_case_multilingual(text: str) -> bool:
        """Check if text appears to be title case across different scripts."""
        # For Latin scripts
        if re.match(r'^[A-Z][a-z]', text):
            return True
        
        # For other scripts, check if it's a short line that could be a title
        if len(text) < 80 and not text.endswith(('.', '。', '।', '؟', '？')):
            return True
        
        return False

    @staticmethod
    def _contains_section_keywords_multilingual(text: str) -> bool:
        """Check if text contains section keywords in any supported language."""
        text_lower = text.lower()
        
        # Check against all language constants
        for lang_code, terms in LanguageConstants.SECTION_HEADERS.items():
            if any(term.lower() in text_lower for term in terms):
                return True
        
        # Check other term types
        for term_dict in [LanguageConstants.FAQ_TERMS, LanguageConstants.POLICY_TERMS, 
                          LanguageConstants.PRICING_TERMS, LanguageConstants.FEATURE_TERMS,
                          LanguageConstants.OVERVIEW_TERMS, LanguageConstants.CONTACT_TERMS]:
            for lang_code, terms in term_dict.items():
                if any(term.lower() in text_lower for term in terms):
                    return True
        
        return False

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
                pattern = rf'(?:^|\n)(?:{re.escape(term)}|{re.escape(term.capitalize())}|{re.escape(term.upper())})\s*\d*[.:\uFF1A]\s*([^\n]*?)(?:\n|$)'
            else:
                # For other scripts
                pattern = rf'(?:^|\n){re.escape(term)}\s*\d*[.:\uFF1A\u3002]\s*([^\n]*?)(?:\n|$)'
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
    def _create_chunked_sections(text: str, language: str, script_group: str,
                                max_section_length: int, min_section_length: int) -> List[Tuple[str, str]]:
        """Create artificial sections by chunking text intelligently with multilingual support."""
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
                # Generate meaningful title from first line or sentences
                title = TextProcessingUtils.extract_section_title_from_content(section_content, section_num)
                sections.append((title, section_content))
                section_num += 1
            
            current_pos = best_break
        
        return sections

    @staticmethod
    def _subdivide_large_section(parent_title: str, content: str, language: str, script_group: str,
                                max_section_length: int, min_section_length: int) -> List[Tuple[str, str]]:
        """Subdivide a large section into smaller manageable sections with multilingual support."""
        if len(content) <= max_section_length:
            return [(parent_title, content)]
        
        print(f"Subdividing large section: {parent_title}")
        
        # Try to find natural breakpoints (paragraphs)
        paragraphs = re.split(r'\n\s*\n', content)
        
        if len(paragraphs) <= 2:
            # No natural breaks, split at sentence boundaries
            return DocumentSectionUtils._split_at_sentences(parent_title, content, language, script_group, max_section_length)
        
        # Group paragraphs into reasonable chunks
        sections = []
        current_chunk = []
        current_length = 0
        chunk_num = 1
        
        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if not paragraph:
                continue
                
            # If adding this paragraph would exceed limit, finalize current chunk
            if current_length + len(paragraph) > max_section_length and current_chunk:
                chunk_content = '\n\n'.join(current_chunk)
                chunk_title = DocumentSectionUtils._create_part_title(parent_title, chunk_num, language) if chunk_num > 1 else parent_title
                sections.append((chunk_title, chunk_content))
                
                current_chunk = [paragraph]
                current_length = len(paragraph)
                chunk_num += 1
            else:
                current_chunk.append(paragraph)
                current_length += len(paragraph)
        
        # Add final chunk
        if current_chunk:
            chunk_content = '\n\n'.join(current_chunk)
            chunk_title = DocumentSectionUtils._create_part_title(parent_title, chunk_num, language) if chunk_num > 1 else parent_title
            sections.append((chunk_title, chunk_content))
        
        return sections

    @staticmethod
    def _split_at_sentences(title: str, content: str, language: str, script_group: str, max_length: int) -> List[Tuple[str, str]]:
        """Split content at sentence boundaries using language-specific sentence endings."""
        # Get language-specific sentence endings
        sentence_endings = DocumentSectionUtils._get_sentence_endings(script_group)
        sentences = DocumentSectionUtils._split_into_sentences(content, sentence_endings)
        
        if not sentences:
            return [(title, content)]
        
        sections = []
        current_chunk = []
        current_length = 0
        chunk_num = 1
        
        for sentence in sentences:
            sentence_with_space = sentence + ' '
            
            # If adding this sentence would exceed limit, finalize current chunk
            if current_length + len(sentence_with_space) > max_length and current_chunk:
                chunk_content = ' '.join(current_chunk)
                chunk_title = DocumentSectionUtils._create_part_title(title, chunk_num, language) if chunk_num > 1 else title
                sections.append((chunk_title, chunk_content))
                
                current_chunk = [sentence]
                current_length = len(sentence_with_space)
                chunk_num += 1
            else:
                current_chunk.append(sentence)
                current_length += len(sentence_with_space)
        
        # Add final chunk
        if current_chunk:
            chunk_content = ' '.join(current_chunk)
            chunk_title = DocumentSectionUtils._create_part_title(title, chunk_num, language) if chunk_num > 1 else title
            sections.append((chunk_title, chunk_content))
        
        return sections

    @staticmethod
    def _split_into_sentences(text: str, sentence_endings: str) -> List[str]:
        """Split text into sentences using language-specific endings."""
        # Escape sentence endings for regex
        endings_pattern = '[' + re.escape(sentence_endings) + ']'
        
        # Split on sentence endings followed by whitespace or line breaks
        sentences = re.split(f'{endings_pattern}+\\s+', text)
        
        # Clean and filter sentences
        cleaned_sentences = []
        for sentence in sentences:
            sentence = sentence.strip()
            if sentence and len(sentence) > 5:  # Minimum sentence length
                cleaned_sentences.append(sentence)
        
        return cleaned_sentences

    @staticmethod
    def _get_sentence_endings(script_group: str) -> str:
        """Get sentence ending characters based on script group."""
        endings = {
            'latin': '.!?',
            'cyrillic': '.!?',
            'arabic': '.!?؟؛',
            'cjk': '。！？：；',
            'devanagari': '।!?.',
            'bengali': '।!?.',
            'dravidian': '।!?.',
            'thai': '.!?',
            'greek': '.!?;',
            'hebrew': '.!?',
            'gurmukhi': '।!?.',
            'gujarati': '।!?.',
            'sinhala': '.!?.',
            'african': '.!?'
        }
        return endings.get(script_group, '.!?')

    @staticmethod
    def _create_part_title(base_title: str, part_num: int, language: str) -> str:
        """Create part title using language-specific part words."""
        part_word = LanguageConstants.PART_WORDS.get(language, 'Part')
        return f"{base_title} - {part_word} {part_num}"

    @staticmethod
    def _add_minimal_overlap(sections: List[Tuple[str, str]], language: str, script_group: str, overlap_sentences: int = 1) -> List[Tuple[str, str]]:
        """Add minimal overlap using language-aware sentence splitting."""
        if len(sections) <= 1 or overlap_sentences <= 0:
            return sections
        
        sentence_endings = DocumentSectionUtils._get_sentence_endings(script_group)
        overlapped_sections = []
        
        for i, (title, content) in enumerate(sections):
            extended_content = content
            
            # Add just last sentence(s) from previous section
            if i > 0 and overlap_sentences > 0:
                prev_content = sections[i-1][1]
                prev_sentences = DocumentSectionUtils._split_into_sentences(prev_content, sentence_endings)
                if len(prev_sentences) >= overlap_sentences:
                    overlap_text = ' '.join(prev_sentences[-overlap_sentences:])
                    # Use language-specific context indicator
                    context_word = DocumentSectionUtils._get_context_word(language)
                    extended_content = f"[{context_word}: {overlap_text}]\n\n{extended_content}"
            
            overlapped_sections.append((title, extended_content))
        
        return overlapped_sections

    @staticmethod
    def _get_context_word(language: str) -> str:
        """Get context indicator word in the specified language."""
        context_words = {
            'en': 'Previous context',
            'es': 'Contexto anterior',
            'fr': 'Contexte précédent',
            'de': 'Vorheriger Kontext',
            'it': 'Contesto precedente',
            'pt': 'Contexto anterior',
            'ru': 'Предыдущий контекст',
            'zh': '上文',
            'ja': '前の文脈',
            'ko': '이전 맥락',
            'ar': 'السياق السابق',
            'hi': 'पिछला संदर्भ',
            'th': 'บริบทก่อนหน้า'
        }
        return context_words.get(language, 'Previous context')

    @staticmethod
    def _ensure_complete_coverage(sections: List[Tuple[str, str]], full_text: str, language: str) -> List[Tuple[str, str]]:
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