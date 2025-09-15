# text_processing_utils.py
import re
from typing import List, Dict, Any, Optional, Tuple

class TextProcessingUtils:
    """Utility class for text processing operations"""
    
    @staticmethod
    def split_into_sentences(text: str, language: str, script_groups: Dict[str, set], sentence_end_markers: Dict[str, List[str]]) -> List[str]:
        """
        Split text into sentences with language-specific rules.
        
        Args:
            text: Text to split
            language: Language code
            script_groups: Script group mappings
            sentence_end_markers: Language-specific sentence end markers
            
        Returns:
            List of sentences
        """
        # Try to use NLTK if available for supported languages
        try:
            import nltk
            nltk_supported = ['en', 'es', 'fr', 'de', 'it', 'nl', 'pt']
            
            if language in nltk_supported:
                try:
                    return nltk.sent_tokenize(text, language)
                except (ImportError, LookupError):
                    pass  # Fall back to regex
        except ImportError:
            pass
        
        # Get script group and sentence markers for this language
        script_group = TextProcessingUtils.get_script_group(language, script_groups)
        end_markers = sentence_end_markers.get(script_group, sentence_end_markers['latin'])
        
        # Escape special regex characters
        escaped_markers = [re.escape(marker) for marker in end_markers]
        
        # Different splitting strategy based on script group
        if script_group == 'cjk':
            # For CJK languages, don't require spaces after punctuation
            pattern = f"([{''.join(escaped_markers)}])"
            parts = re.split(pattern, text)
            
            # Recombine parts (text + punctuation)
            sentences = []
            current = ""
            for i, part in enumerate(parts):
                if i % 2 == 0:  # Text
                    current += part
                else:  # Punctuation
                    current += part
                    if current.strip():
                        sentences.append(current.strip())
                    current = ""
            
            # Add any remaining text
            if current.strip():
                sentences.append(current.strip())
                
            return sentences
            
        elif script_group == 'thai':
            # For Thai, split on specific punctuation or double spaces (common separator)
            pattern = f"(?<=[{''.join(escaped_markers)}])|(?<=\\s\\s)"
            return [s.strip() for s in re.split(pattern, text) if s.strip()]
            
        else:
            # For other scripts, assume punctuation followed by space
            pattern = f"(?<=[{''.join(escaped_markers)}])\\s+"
            sentences = re.split(pattern, text)
            
            # If we got very few sentences, try a less strict pattern
            if len(sentences) <= 1 and len(text) > 200:
                pattern = f"(?<=[{''.join(escaped_markers)}])"
                sentences = re.split(pattern, text)
            
            return [s.strip() for s in sentences if s.strip()]

    @staticmethod
    def get_script_group(language: str, script_groups: Dict[str, set]) -> str:
        """
        Get the script group for a language.
        
        Args:
            language: Language code
            script_groups: Script group mappings
            
        Returns:
            Script group name
        """
        for group, langs in script_groups.items():
            if language in langs:
                return group
        return 'latin'  # Default to Latin script

    @staticmethod
    def extract_fallback_keywords(text: str, max_keywords: int) -> List[str]:
        """
        Extract keywords using simple frequency analysis
        
        Args:
            text: Text to process
            max_keywords: Maximum keywords to extract
            
        Returns:
            List of keywords
        """
        # Simple keyword extraction
        words = text.lower().split()
        word_freq = {}
        
        for word in words:
            word = word.strip('.,;:!?()[]{}')
            if len(word) > 3 and word not in ['the', 'and', 'or', 'but', 'with', 'from', 'this', 'that', 'they', 'have', 'been', 'were']:
                word_freq[word] = word_freq.get(word, 0) + 1
        
        # Get most frequent words as keywords
        keywords = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:max_keywords]
        keyword_list = [word for word, freq in keywords]
        
        return keyword_list

    @staticmethod
    def create_fallback_summary(text: str, section_title: str) -> str:
        """Create a simple fallback summary"""
        # Use first sentence or first 100 characters
        sentences = text.split('.')
        if sentences and len(sentences[0].strip()) > 10:
            summary = sentences[0].strip() + '.'
        else:
            summary = text[:100].strip() + '...'
        
        # Add section context if available
        if section_title and section_title != "General":
            summary = f"Content from {section_title}: {summary}"
        
        return summary

    @staticmethod
    def is_list_item(line: str) -> bool:
        """
        Check if a line is a list item (numbered, bulleted, lettered, etc.).
        
        Args:
            line: Text line to check
            
        Returns:
            True if line is a list item
        """
        line = line.strip()
        if not line:
            return False
        
        # Common list patterns across languages
        list_patterns = [
            # Numbered lists
            r'^\d+[\.\)]\s+',           # 1. or 1)
            r'^\d+\.\d+[\.\)]\s+',      # 1.1. or 1.1)
            
            # Lettered lists
            r'^[a-zA-Z][\.\)]\s+',      # a. or a)
            r'^[IVX]+[\.\)]\s+',        # I. or I) (Roman numerals)
            
            # Bullet points
            r'^[-\*\+•·◦▪▫]\s+',        # -, *, +, •, ·, ◦, ▪, ▫
            
            # Parenthetical numbers/letters
            r'^\([0-9a-zA-Z]+\)\s+',    # (1) or (a)
            
            # Indented items (common in structured docs)
            r'^\s{2,}[-\*\+•·]\s+',     # Indented bullets
            r'^\s{2,}\d+[\.\)]\s+',     # Indented numbers
            
            # Special characters (other languages)
            r'^[○●◯◉]\s+',              # Circle bullets
            r'^[①②③④⑤⑥⑦⑧⑨⑩]\s*',      # Circled numbers
            r'^[⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽]\s*',      # Parenthesized numbers
            
            # Arabic/Persian numbering
            r'^[١٢٣٤٥٦٧٨٩٠]+[\.\)]\s+',
            
            # Devanagari numbering
            r'^[०१२३४५६७८९]+[\.\)]\s+',
            
            # Chinese/Japanese numbering
            r'^[一二三四五六七八九十百千]+[\.\)、]\s+',
            r'^第[一二三四五六七八九十百千]+[章节条款项]\s+',
            
            # Task/checkbox lists
            r'^\[\s*[xX✓✗]?\s*\]\s+',   # [ ], [x], [✓]
            r'^☐\s+|^☑\s+|^☒\s+',        # Checkbox symbols
        ]
        
        for pattern in list_patterns:
            if re.match(pattern, line, re.UNICODE):
                return True
        
        return False

    @staticmethod
    def extract_section_title_from_content(content: str, section_num: int) -> str:
        """
        Extract a meaningful title from section content.
        
        Args:
            content: Section content
            section_num: Section number for fallback
            
        Returns:
            Section title
        """
        lines = content.split('\n')
        
        # Try first non-empty line as title
        for line in lines[:3]:  # Check first 3 lines
            line = line.strip()
            if line and len(line) > 0:
                # Check if this line looks like a title
                if TextProcessingUtils.looks_like_title(line):
                    return line[:100]  # Limit title length
        
        # Try to find a line that's shorter and might be a header
        for line in lines[:5]:
            line = line.strip()
            if line and 5 < len(line) < 80 and not line.endswith(('.', '。', '।', '؟', '?', '!')):
                return line
        
        # Extract key words from first sentence for auto-title
        first_sentence = TextProcessingUtils.get_first_sentence(content)
        if first_sentence and 10 < len(first_sentence) < 100:
            return first_sentence
        
        # Fallback to generic title
        return f"Section {section_num}"

    @staticmethod
    def looks_like_title(line: str) -> bool:
        """
        Determine if a line looks like a title/header.
        
        Args:
            line: Text line to check
            
        Returns:
            True if line looks like a title
        """
        line = line.strip()
        
        # Empty or too long
        if not line or len(line) > 120:
            return False
        
        # All caps might be a title
        if line.isupper() and len(line) > 3:
            return True
        
        # Starts with capital and doesn't end with sentence ending
        if (line[0].isupper() and 
            not line.endswith(('.', '。', '।', '؟', '?', '!', ',', ';', ':')) and
            5 < len(line) < 80):
            return True
        
        # Contains title-like patterns
        title_patterns = [
            r'^(Chapter|Section|Part|Unit|Module|Lesson|Topic)\s+\d+',
            r'^\d+[\.\)]\s+[A-Z]',
            r'^[A-Z][^.!?]*[A-Z][^.!?]*$',  # Multiple capitals, no sentence endings
            r'^\s*[IVX]+\.\s+[A-Z]',  # Roman numerals
        ]
        
        for pattern in title_patterns:
            if re.match(pattern, line, re.IGNORECASE):
                return True
        
        return False

    @staticmethod
    def get_first_sentence(text: str) -> str:
        """Extract the first sentence from text."""
        # Find first sentence ending
        sentence_endings = '.!?。।؟'
        
        for i, char in enumerate(text):
            if char in sentence_endings:
                # Make sure it's not an abbreviation
                if i + 1 < len(text) and text[i + 1] in ' \n\t':
                    return text[:i + 1].strip()
        
        # If no sentence ending found, return first 100 characters
        return text[:100].strip() + '...' if len(text) > 100 else text.strip()