import re
from typing import List, Dict, Any, Optional, Tuple
from pydantic import BaseModel
from language_constants import LanguageConstants
from text_processing_utils import TextProcessingUtils
from document_section_utils import DocumentSectionUtils

class ChunkMetadata(BaseModel):
    hasQuestion: bool = False
    documentType: str = "general"
    sectionType: str = "general"
    language: str = "en"
    qaFormat: Optional[str] = None
    question: Optional[str] = None
    answer: Optional[str] = None
    relatedQuestion: Optional[str] = None
    relatedAnswer: Optional[str] = None
    chunkIndex: int = 0
    totalChunks: int = 1
    sectionIndex: int = 0
    qaPairIndex: Optional[int] = None
    clauseIndex: Optional[int] = None
    hasOverlap: bool = False
    qaCount: int = 0
    contentScore: float = 1.0

class Chunk(BaseModel):
    text: str
    section: str
    type: str
    metadata: ChunkMetadata

class ImprovedChunkingUtils:
    """Complete improved utilities for intelligent text chunking with multilingual support"""
    
    @staticmethod
    def extract_chunks_with_section_analysis(text: str, language: str, sections: List[Tuple[str, str]]) -> List[Chunk]:
        """
        Extract semantic chunks with section-based document type detection.
        Creates fewer, more meaningful chunks with proper context preservation.
        """
        chunks = []
        
        # Analyze each section to determine its type
        section_analysis = []
        for section_idx, (section_title, section_content) in enumerate(sections):
            if not section_content.strip():
                continue
                
            section_type = ImprovedChunkingUtils._analyze_section_type(section_content, section_title, language)
            section_analysis.append((section_idx, section_title, section_content, section_type))
        
        # Determine overall document type based on section analysis
        overall_doc_type = ImprovedChunkingUtils._determine_overall_document_type(section_analysis)
        
        # Process each section with context-aware chunking
        for section_idx, section_title, section_content, section_type in section_analysis:
            section_chunks = ImprovedChunkingUtils._process_section_intelligently(
                section_content, section_title, section_type, overall_doc_type, language, section_idx
            )
            chunks.extend(section_chunks)
        
        # Post-process chunks to optimize them
        optimized_chunks = ImprovedChunkingUtils._optimize_chunks(chunks, overall_doc_type)
        
        # Update chunk indices and total count
        for i, chunk in enumerate(optimized_chunks):
            chunk.metadata.chunkIndex = i
            chunk.metadata.totalChunks = len(optimized_chunks)
            chunk.metadata.documentType = overall_doc_type
            
        return optimized_chunks

    @staticmethod
    def _analyze_section_type(section_content: str, section_title: str, language: str) -> str:
        """Analyze section to determine its specific type using language constants"""
        content_lower = section_content.lower()
        title_lower = section_title.lower()
        
        # Check for FAQ indicators
        faq_terms = LanguageConstants.FAQ_TERMS.get(language, LanguageConstants.FAQ_TERMS['en'])
        if any(term in title_lower for term in faq_terms) or any(term in content_lower for term in faq_terms):
            return 'faq'
        
        # Check for Q&A patterns
        qa_patterns = ImprovedChunkingUtils._count_qa_patterns(section_content, language)
        if qa_patterns >= 3:
            return 'faq'
        
        # Check for policy indicators
        policy_terms = LanguageConstants.POLICY_TERMS.get(language, LanguageConstants.POLICY_TERMS['en'])
        if any(term in title_lower for term in policy_terms):
            return 'policy'
        
        # Check for pricing indicators
        pricing_terms = LanguageConstants.PRICING_TERMS.get(language, LanguageConstants.PRICING_TERMS['en'])
        if any(term in title_lower for term in pricing_terms):
            return 'pricing'
        
        # Check for features
        feature_terms = LanguageConstants.FEATURE_TERMS.get(language, LanguageConstants.FEATURE_TERMS['en'])
        if any(term in title_lower for term in feature_terms):
            return 'features'
        
        # Check for overview
        overview_terms = LanguageConstants.OVERVIEW_TERMS.get(language, LanguageConstants.OVERVIEW_TERMS['en'])
        if any(term in title_lower for term in overview_terms):
            return 'overview'
        
        # Check for contact
        contact_terms = LanguageConstants.CONTACT_TERMS.get(language, LanguageConstants.CONTACT_TERMS['en'])
        if any(term in title_lower for term in contact_terms):
            return 'contact'
        
        return 'general'

    @staticmethod
    def _count_qa_patterns(text: str, language: str) -> int:
        """Count Q&A patterns in text"""
        qa_count = 0
        qa_markers = LanguageConstants.QA_MARKERS.get(language, LanguageConstants.QA_MARKERS['en'])
        
        for q_marker, a_marker in qa_markers:
            for separator in [':', '.', ' ']:
                q_pattern = f"{q_marker}{separator}"
                a_pattern = f"{a_marker}{separator}"
                
                pattern = rf'(?:^|\n)\s*{re.escape(q_pattern)}\s*(.*?)(?:\n|\r\n?)\s*{re.escape(a_pattern)}\s*(.*?)(?=\n\s*{re.escape(q_pattern)}|\Z)'
                matches = re.finditer(pattern, text, re.DOTALL | re.IGNORECASE)
                qa_count += len(list(matches))
        
        return qa_count

    @staticmethod
    def _determine_overall_document_type(section_analysis: List[Tuple[int, str, str, str]]) -> str:
        """Determine overall document type based on section analysis"""
        section_types = [section_type for _, _, _, section_type in section_analysis]
        type_counts = {}
        
        for section_type in section_types:
            type_counts[section_type] = type_counts.get(section_type, 0) + 1
        
        total_sections = len(section_types)
        
        # If more than 50% FAQ, it's FAQ document
        if type_counts.get('faq', 0) / total_sections > 0.5:
            return 'faq'
        
        # If more than 50% policy, it's policy document
        if type_counts.get('policy', 0) / total_sections > 0.5:
            return 'policy'
        
        # If mixed content (3+ different types), it's knowledge base
        unique_types = set(section_types) - {'general'}
        if len(unique_types) >= 3:
            return 'knowledge_base'
        
        # If predominantly one type, use that
        if unique_types:
            most_common_type = max(type_counts.items(), key=lambda x: x[1])[0]
            if most_common_type != 'general':
                return most_common_type
        
        return 'general'

    @staticmethod
    def _process_section_intelligently(section_content: str, section_title: str, section_type: str, 
                                     overall_doc_type: str, language: str, section_idx: int) -> List[Chunk]:
        """Process section intelligently based on its type - REDUCED CHUNKING"""
        chunks = []
        
        # Create ONE consolidated section chunk instead of multiple
        if section_type == 'faq':
            # Extract Q&A pairs and consolidate them
            qa_pairs = ImprovedChunkingUtils._extract_qa_pairs_improved(section_content, language)
            if qa_pairs:
                # Create ONE FAQ chunk with all Q&As
                consolidated_text = f"FAQ Section: {section_title}\n\n"
                for i, (question, answer) in enumerate(qa_pairs):
                    consolidated_text += f"Q{i+1}: {question}\nA{i+1}: {answer}\n\n"
                
                faq_chunk = Chunk(
                    text=consolidated_text.strip(),
                    section=section_title,
                    type="faq_section",
                    metadata=ChunkMetadata(
                        hasQuestion=True,
                        documentType=overall_doc_type,
                        sectionType='faq',
                        language=language,
                        qaFormat="consolidated",
                        sectionIndex=section_idx,
                        qaCount=len(qa_pairs),
                        contentScore=1.0
                    )
                )
                chunks.append(faq_chunk)
            else:
                # No Q&A pairs found, treat as regular content
                chunks.append(ImprovedChunkingUtils._create_single_section_chunk(
                    section_content, section_title, section_type, overall_doc_type, language, section_idx
                ))
        
        elif section_type == 'policy':
            # Create ONE consolidated policy chunk
            chunks.append(ImprovedChunkingUtils._create_single_section_chunk(
                section_content, section_title, section_type, overall_doc_type, language, section_idx
            ))
        
        else:
            # For other section types, create ONE chunk per section (not multiple)
            # Only split if section is extremely large (> 6000 chars)
            if len(section_content) > 6000:
                # Split into max 2-3 chunks for very large sections
                chunks.extend(ImprovedChunkingUtils._create_minimal_chunks(
                    section_content, section_title, section_type, overall_doc_type, language, section_idx
                ))
            else:
                chunks.append(ImprovedChunkingUtils._create_single_section_chunk(
                    section_content, section_title, section_type, overall_doc_type, language, section_idx
                ))
        
        return chunks

    @staticmethod
    def _create_single_section_chunk(content: str, title: str, section_type: str, doc_type: str, language: str, section_idx: int) -> Chunk:
        """Create a single chunk for an entire section"""
        return Chunk(
            text=f"Section: {title}\n\n{content}",
            section=title,
            type=f"{section_type}_section",
            metadata=ChunkMetadata(
                hasQuestion=(section_type == 'faq'),
                documentType=doc_type,
                sectionType=section_type,
                language=language,
                sectionIndex=section_idx,
                contentScore=1.0
            )
        )

    @staticmethod
    def _create_minimal_chunks(content: str, title: str, section_type: str, doc_type: str, language: str, section_idx: int) -> List[Chunk]:
        """Create minimal chunks for very large sections (max 2-3 chunks)"""
        chunks = []
        
        # Split into paragraphs
        paragraphs = [p.strip() for p in re.split(r'\n\s*\n', content) if p.strip()]
        if not paragraphs:
            return [ImprovedChunkingUtils._create_single_section_chunk(content, title, section_type, doc_type, language, section_idx)]
        
        # Target: max 3 chunks, each around 2000-3000 chars
        target_chunks = min(3, max(1, len(content) // 2500))
        paras_per_chunk = max(1, len(paragraphs) // target_chunks)
        
        chunk_num = 1
        for i in range(0, len(paragraphs), paras_per_chunk):
            chunk_paragraphs = paragraphs[i:i + paras_per_chunk]
            chunk_content = '\n\n'.join(chunk_paragraphs)
            
            chunk_title = f"{title} - Part {chunk_num}" if target_chunks > 1 else title
            
            chunk = Chunk(
                text=f"Section: {chunk_title}\n\n{chunk_content}",
                section=title,
                type=f"{section_type}_section",
                metadata=ChunkMetadata(
                    hasQuestion=(section_type == 'faq'),
                    documentType=doc_type,
                    sectionType=section_type,
                    language=language,
                    sectionIndex=section_idx,
                    contentScore=1.0
                )
            )
            chunks.append(chunk)
            chunk_num += 1
        
        return chunks

    @staticmethod
    def _extract_qa_pairs_improved(text: str, language: str) -> List[Tuple[str, str]]:
        """Improved Q&A extraction"""
        qa_pairs = []
        qa_markers = LanguageConstants.QA_MARKERS.get(language, LanguageConstants.QA_MARKERS['en'])
        
        for q_marker, a_marker in qa_markers:
            for separator in [':', '.', ' ']:
                q_pattern = f"{q_marker}{separator}"
                a_pattern = f"{a_marker}{separator}"
                
                pattern = rf'(?:^|\n)\s*{re.escape(q_pattern)}\s*(.*?)(?:\n|\r\n?)\s*{re.escape(a_pattern)}\s*(.*?)(?=\n\s*{re.escape(q_pattern)}|\Z)'
                
                matches = re.finditer(pattern, text, re.DOTALL | re.IGNORECASE)
                for match in matches:
                    question = match.group(1).strip()
                    answer = match.group(2).strip()
                    
                    if question and answer and len(question) > 10 and len(answer) > 10:
                        qa_pairs.append((question, answer))
        
        return qa_pairs

    @staticmethod
    def _optimize_chunks(chunks: List[Chunk], overall_doc_type: str) -> List[Chunk]:
        """Optimize chunks by removing duplicates and improving quality"""
        if not chunks:
            return chunks
        
        # Remove very short chunks
        filtered_chunks = []
        for chunk in chunks:
            if len(chunk.text.strip()) >= 100:  # Increased minimum length
                filtered_chunks.append(chunk)
        
        # Remove near-duplicate chunks based on content similarity
        final_chunks = []
        seen_content = set()
        
        for chunk in filtered_chunks:
            # Create a signature based on first 200 chars
            content_signature = re.sub(r'\s+', ' ', chunk.text.lower()[:200]).strip()
            
            if content_signature not in seen_content:
                seen_content.add(content_signature)
                final_chunks.append(chunk)
        
        return final_chunks


    # Usage example to integrate both classes:
    def process_document_complete(text: str, language: str = 'en') -> List[Chunk]:
        """
        Complete document processing pipeline that produces optimal chunks.
        
        Expected output for your document: 8-12 chunks instead of 45
        """
        
        # Step 1: Identify sections using improved section detection
        sections = DocumentSectionUtils.identify_document_sections(
            text=text,
            language=language,
            max_section_length=4000,  # Higher threshold
            min_section_length=300,   # Higher minimum
            overlap_sentences=1       # Minimal overlap
        )
        
        print(f"✅ Found {len(sections)} document sections")
        
        # Step 2: Create intelligent chunks from sections
        chunks = ImprovedChunkingUtils.extract_chunks_with_section_analysis(
            text=text,
            language=language,
            sections=sections
        )
        
        print(f"✅ Created {len(chunks)} optimized chunks")
        
        return chunks


    # Expected results for your Convonest document:
    """
    Instead of 45 chunks, you should get approximately 8-12 chunks:

    1. "Platform Overview" (1 chunk)
    2. "Platform Features" (1-2 chunks if very large)
    3. "Pricing Plans" (1 chunk)
    4. "Core Features Included in All Plans" (1 chunk)
    5. "Why Choose Convonest" (1 chunk)
    6. "Getting Started Guide" (1 chunk)
    7. "Technical Specifications" (1 chunk)
    8. "Leadership Team" (1 chunk)
    9. "Frequently Asked Questions" (1 consolidated FAQ chunk)
    10. "Contact Information" (1 chunk)

    Total: ~10 chunks with minimal duplication and proper context preservation
    """