import re
from typing import List, Dict, Any, Optional, Tuple
from pydantic import BaseModel
from language_constants import LanguageConstants
from text_processing_utils import TextProcessingUtils

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
    """Complete improved utilities for intelligent text chunking with section-based analysis"""
    
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
        """Process section intelligently based on its type"""
        chunks = []
        
        # Always create section-level chunk for context
        section_chunk = Chunk(
            text=f"Section: {section_title}\n\n{section_content}",
            section=section_title,
            type="section_overview",
            metadata=ChunkMetadata(
                hasQuestion=False,
                documentType=overall_doc_type,
                sectionType=section_type,
                language=language,
                chunkIndex=0,
                totalChunks=0,
                sectionIndex=section_idx,
                contentScore=1.0
            )
        )
        chunks.append(section_chunk)
        
        # Process based on section type
        if section_type == 'faq':
            faq_chunks = ImprovedChunkingUtils._process_faq_section_consolidated(
                section_content, section_title, language, section_idx
            )
            chunks.extend(faq_chunks)
            
        elif section_type == 'policy':
            policy_chunks = ImprovedChunkingUtils._process_policy_section_consolidated(
                section_content, section_title, language, section_idx
            )
            chunks.extend(policy_chunks)
            
        else:
            content_chunks = ImprovedChunkingUtils._create_consolidated_chunks(
                section_content, section_title, section_type, language, section_idx
            )
            chunks.extend(content_chunks)
        
        return chunks

    @staticmethod
    def _process_faq_section_consolidated(section_content: str, section_title: str, language: str, section_idx: int) -> List[Chunk]:
        """Process FAQ sections by consolidating related Q&As"""
        chunks = []
        
        # Extract all Q&A pairs
        qa_pairs = ImprovedChunkingUtils._extract_qa_pairs_improved(section_content, language)
        
        if qa_pairs:
            # Group Q&As into consolidated chunks (6 Q&As per chunk)
            chunk_size = 6
            
            for i in range(0, len(qa_pairs), chunk_size):
                batch = qa_pairs[i:i + chunk_size]
                
                consolidated_text = f"FAQ Section: {section_title}\n\n"
                for j, (question, answer) in enumerate(batch):
                    consolidated_text += f"Q{i+j+1}: {question}\nA{i+j+1}: {answer}\n\n"
                
                faq_chunk = Chunk(
                    text=consolidated_text.strip(),
                    section=section_title,
                    type="faq_consolidated",
                    metadata=ChunkMetadata(
                        hasQuestion=True,
                        documentType='faq',
                        sectionType='faq',
                        language=language,
                        qaFormat="consolidated",
                        chunkIndex=len(chunks),
                        totalChunks=0,
                        sectionIndex=section_idx,
                        qaCount=len(batch),
                        contentScore=1.0
                    )
                )
                chunks.append(faq_chunk)
        else:
            # No Q&A pairs found, treat as regular content
            content_chunks = ImprovedChunkingUtils._create_consolidated_chunks(
                section_content, section_title, 'faq', language, section_idx
            )
            chunks.extend(content_chunks)
        
        return chunks

    @staticmethod
    def _process_policy_section_consolidated(section_content: str, section_title: str, language: str, section_idx: int) -> List[Chunk]:
        """Process policy sections by consolidating related clauses"""
        chunks = []
        
        script_group = TextProcessingUtils.get_script_group(language, LanguageConstants.SCRIPT_GROUPS)
        clauses = ImprovedChunkingUtils._extract_policy_clauses_improved(section_content, script_group)
        
        if clauses:
            # Group clauses into chunks (4 clauses per chunk)
            chunk_size = 4
            
            for i in range(0, len(clauses), chunk_size):
                batch = clauses[i:i + chunk_size]
                
                consolidated_text = f"Policy Section: {section_title}\n\n"
                for j, clause in enumerate(batch):
                    consolidated_text += f"Clause {i+j+1}: {clause}\n\n"
                
                policy_chunk = Chunk(
                    text=consolidated_text.strip(),
                    section=section_title,
                    type="policy_consolidated",
                    metadata=ChunkMetadata(
                        hasQuestion=False,
                        documentType='policy',
                        sectionType='policy',
                        language=language,
                        chunkIndex=len(chunks),
                        totalChunks=0,
                        sectionIndex=section_idx,
                        contentScore=1.0
                    )
                )
                chunks.append(policy_chunk)
        else:
            content_chunks = ImprovedChunkingUtils._create_consolidated_chunks(
                section_content, section_title, 'policy', language, section_idx
            )
            chunks.extend(content_chunks)
        
        return chunks

    @staticmethod
    def _create_consolidated_chunks(content: str, section_title: str, section_type: str, language: str, section_idx: int) -> List[Chunk]:
        """Create consolidated chunks for non-FAQ, non-policy content"""
        chunks = []
        
        paragraphs = [p.strip() for p in re.split(r'\n\s*\n', content) if p.strip()]
        if not paragraphs:
            return chunks
        
        # Target: 800-1200 characters per chunk
        target_size = 1000
        max_size = 1200
        min_size = 600
        
        current_chunk = []
        current_size = 0
        
        for paragraph in paragraphs:
            para_size = len(paragraph)
            
            if para_size > max_size:
                # Save current chunk if exists
                if current_chunk:
                    chunk_text = "\n\n".join(current_chunk)
                    chunk = ImprovedChunkingUtils._create_content_chunk(
                        chunk_text, section_title, section_type, language, section_idx, len(chunks)
                    )
                    chunks.append(chunk)
                    current_chunk = []
                    current_size = 0
                
                # Split large paragraph into sentences
                sentences = TextProcessingUtils.split_into_sentences(
                    paragraph, language, LanguageConstants.SCRIPT_GROUPS, LanguageConstants.SENTENCE_END_MARKERS
                )
                
                sentence_chunk = []
                sentence_size = 0
                
                for sentence in sentences:
                    sentence = sentence.strip()
                    if not sentence:
                        continue
                        
                    sent_len = len(sentence)
                    
                    if sentence_size + sent_len > max_size and sentence_chunk:
                        chunk_text = " ".join(sentence_chunk)
                        chunk = ImprovedChunkingUtils._create_content_chunk(
                            chunk_text, section_title, section_type, language, section_idx, len(chunks)
                        )
                        chunks.append(chunk)
                        sentence_chunk = [sentence]
                        sentence_size = sent_len
                    else:
                        sentence_chunk.append(sentence)
                        sentence_size += sent_len
                
                if sentence_chunk:
                    chunk_text = " ".join(sentence_chunk)
                    chunk = ImprovedChunkingUtils._create_content_chunk(
                        chunk_text, section_title, section_type, language, section_idx, len(chunks)
                    )
                    chunks.append(chunk)
                
            elif current_size + para_size > max_size and current_chunk:
                chunk_text = "\n\n".join(current_chunk)
                chunk = ImprovedChunkingUtils._create_content_chunk(
                    chunk_text, section_title, section_type, language, section_idx, len(chunks)
                )
                chunks.append(chunk)
                current_chunk = [paragraph]
                current_size = para_size
            else:
                current_chunk.append(paragraph)
                current_size += para_size
        
        # Add final chunk
        if current_chunk:
            chunk_text = "\n\n".join(current_chunk)
            if len(chunk_text.strip()) >= min_size:
                chunk = ImprovedChunkingUtils._create_content_chunk(
                    chunk_text, section_title, section_type, language, section_idx, len(chunks)
                )
                chunks.append(chunk)
        
        return chunks

    @staticmethod
    def _create_content_chunk(text: str, section_title: str, section_type: str, language: str, 
                             section_idx: int, chunk_idx: int) -> Chunk:
        """Create standardized content chunk"""
        return Chunk(
            text=text,
            section=section_title,
            type=f"{section_type}_content",
            metadata=ChunkMetadata(
                hasQuestion=False,
                documentType='general',
                sectionType=section_type,
                language=language,
                chunkIndex=chunk_idx,
                totalChunks=0,
                sectionIndex=section_idx,
                contentScore=1.0
            )
        )

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
    def _extract_policy_clauses_improved(text: str, script_group: str) -> List[str]:
        """Improved policy clause extraction"""
        clauses = []
        
        if script_group == 'cjk':
            clause_pattern = r'(?:^|\n)\s*(?:\d+|[一二三四五六七八九十]+)[\.．、]\s*(.*?)(?=\n\s*(?:\d+|[一二三四五六七八九十]+)[\.．、]|\Z)'
        elif script_group in ['devanagari', 'bengali', 'dravidian', 'gurmukhi', 'gujarati']:
            clause_pattern = r'(?:^|\n)\s*(?:\d+|[१२३४५६७८९०]+)[\.।]\s*(.*?)(?=\n\s*(?:\d+|[१२३४५६७८९०]+)[\.।]|\Z)'
        elif script_group == 'arabic':
            clause_pattern = r'(?:^|\n)\s*(?:\d+|[١٢٣٤٥٦٧٨٩٠]+)[\.،]\s*(.*?)(?=\n\s*(?:\d+|[١٢٣٤٥٦٧٨٩٠]+)[\.،]|\Z)'
        else:
            clause_pattern = r'(?:^|\n)\s*\d+\.\s*(.*?)(?=\n\s*\d+\.|\Z)'
        
        for match in re.finditer(clause_pattern, text, re.DOTALL):
            clause = match.group(1).strip()
            if clause and len(clause) > 20:
                clauses.append(clause)
        
        return clauses

    @staticmethod
    def _optimize_chunks(chunks: List[Chunk], overall_doc_type: str) -> List[Chunk]:
        """Optimize chunks by removing duplicates and improving quality"""
        if not chunks:
            return chunks
        
        # Remove very short chunks
        filtered_chunks = []
        for chunk in chunks:
            if len(chunk.text.strip()) >= 50:
                filtered_chunks.append(chunk)
        
        # Remove near-duplicate chunks
        final_chunks = []
        seen_texts = set()
        
        for chunk in filtered_chunks:
            simplified = re.sub(r'\s+', ' ', chunk.text.lower()[:200])
            
            if simplified not in seen_texts:
                seen_texts.add(simplified)
                final_chunks.append(chunk)
        
        return final_chunks