import re
import json
import logging
import asyncio
import datetime
import uuid
import os
import signal
import sys
import httpx
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import List, Dict, Any, Optional, Tuple, Set
from sentence_transformers import SentenceTransformer
from elasticsearch import AsyncElasticsearch
from confluent_kafka import Consumer, Producer, KafkaError
from pydantic import BaseModel
from config import KAFKA_CONFIG,ES_CONFIG,MISTRAL_CONFIG
from libaryLanguage import LibraryLanguageDetector
from contextvars import ContextVar


tracking_id_var = ContextVar("X-Tracking-ID", default="NA")


class DataStorageDto(BaseModel):
    tenantId: str
    storedIds: List[str] 
    isDone: bool
    dataType: str

class ChunkMetadata(BaseModel):
    hasQuestion: bool = False
    documentType: str = "general"
    language: str = "en"
    qaFormat: Optional[str] = None
    question: Optional[str] = None
    chunkIndex: int = 0
    totalChunks: int = 1
    sectionIndex: int = 0

class Chunk(BaseModel):
    text: str
    section: str
    type: str
    metadata: ChunkMetadata

from logger_config import get_logger
logger = get_logger(__name__)

class MultilingualMessageProcessor:
    
    def __init__(self, es_config: Dict[str, Any], kafka_config: Dict[str, Any], model_path: Optional[str] = None):
        """
        Initialize the multilingual message processor with support for 70+ languages.
        
        Args:
            es_config: Elasticsearch configuration.
            kafka_config: Kafka configuration.
            model_path: Optional path to a local SentenceTransformer model.
        """

        # Setup signal handlers for graceful shutdown
        logger.info("=" * 60)
        logger.info("INITIALIZING MULTILINGUAL MESSAGE PROCESSOR")
        logger.info("=" * 60)

        # Setup signal handlers for graceful shutdown
        self.shutdown_requested = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        logger.info("✓ Signal handlers configured for graceful shutdown")
        
        self.es_config = es_config
        self.kafka_config = kafka_config
        self.http_client = httpx.AsyncClient(timeout=300.0)
        # Initialize SentenceTransformer with multilingual model
        logger.info("Initializing SentenceTransformer model...")
        model_name = 'paraphrase-multilingual-mpnet-base-v2'
        try:
            if model_path:
                logger.info(f"Loading model from local path: {model_path}")
                self.st_model = SentenceTransformer(model_path)
                logger.info("✓ SentenceTransformer model loaded successfully from local path")
            else:
                logger.info(f"Loading model '{model_name}' from Hugging Face...")
                self.st_model = SentenceTransformer(model_name)
                logger.info("✓ SentenceTransformer model loaded successfully from Hugging Face")
        except Exception as e:
            logger.error(f"✗ CRITICAL: Failed to load SentenceTransformer model: {e}")
            logger.error("System cannot proceed without the model. Shutting down...")
            raise
            

         # Initialize async Elasticsearch client
        logger.info("Initializing Elasticsearch connection...")
        logger.info(f"Elasticsearch hosts: {es_config['hosts']}")
        logger.info(f"Elasticsearch username: {es_config['username']}")
        logger.info(f"Elasticsearch SSL verification: {es_config.get('verify_certs', True)}")
        
        try:
            self.es_client = AsyncElasticsearch(
                es_config['hosts'],
                basic_auth=(es_config['username'], es_config['password']),
                verify_certs=es_config.get('verify_certs', True),
                ssl_show_warn=es_config.get('ssl_show_warn', True),
                ca_certs=es_config.get('ca_certs'),
                retry_on_timeout=True,
                max_retries=3
            )
            logger.info("✓ Elasticsearch client initialized successfully")
        except Exception as e:
            logger.error(f"✗ CRITICAL: Failed to initialize Elasticsearch client: {e}")
            raise
        
        # Kafka configuration
        logger.info("Configuring Kafka connections...")
        logger.info(f"Kafka bootstrap servers: {kafka_config['bootstrap_servers']}")
        logger.info(f"Kafka consumer group ID: {kafka_config['group_id']}")
        
        self.consumer_config = {
            'bootstrap.servers': kafka_config['bootstrap_servers'],
            'group.id': kafka_config['group_id'],
            'auto.offset.reset': kafka_config.get('auto_offset_reset', 'earliest'),
            'enable.auto.commit': True,
            'session.timeout.ms': 45000,
            'heartbeat.interval.ms': 15000,
            'request.timeout.ms': 65000
        }
        
        self.producer_config = {
            'bootstrap.servers': kafka_config['bootstrap_servers']
        }
        
        
        # Kafka topics
        self.document_request_topic = kafka_config['document_storage_request_topic']
        self.document_dlq_topic = kafka_config['document_storage_request_dlq_topic']
        
        self.faq_request_topic = kafka_config['faq_storage_request_topic']
        self.faq_dlq_topic = kafka_config['faq_storage_request_dlq_topic']
        self.response_topic = kafka_config['vector_storage_response_topic']
        
        logger.info("Kafka topics configured:")
        logger.info(f"  - Document request topic: {self.document_request_topic}")
        logger.info(f"  - Document DLQ topic: {self.document_dlq_topic}")
        logger.info(f"  - FAQ request topic: {self.faq_request_topic}")
        logger.info(f"  - FAQ DLQ topic: {self.faq_dlq_topic}")
        logger.info(f"  - Response topic: {self.response_topic}")

        # Initialize Kafka producer
        self.producer = None

        logger.info("Initializing language detector...")
        try:
            self.libraryDetector = LibraryLanguageDetector()
            logger.info("✓ Language detector initialized successfully")
        except Exception as e:
            logger.error(f"✗ Failed to initialize language detector: {e}")
            logger.warning("Proceeding without language detector - some features may be limited")



        # Define language script groupings
        self.SCRIPT_GROUPS = {
            'latin': {'sl', 'pl', 'de', 'ro', 'tr', 'no', 'sv', 'pt', 'pt-br', 'hr', 
                     'bs', 'fr', 'es', 'sk', 'it', 'nl', 'cs', 'fi', 'lt', 'da', 'hu', 
                     'af', 'ca', 'sq', 'gl', 'en', 'en-ca', 'en-au', 'en-gb', 'et', 'lv', 'id', 'ms'},
                     
            'cyrillic': {'ru', 'uk', 'bg', 'mk', 'sr-cyr', 'kk'},
            
            'devanagari': {'hi', 'ne', 'mr'},
            
            'arabic': {'ar', 'fa', 'ur', 'ps'},
            
            'cjk': {'zh', 'zh-tw', 'ja', 'ko'},
            
            'thai': {'th'},
            
            'greek': {'el'},
            
            'hebrew': {'he'},
            
            'bengali': {'bn'},
            
            'dravidian': {'ta', 'te', 'kn', 'ml'},
            
            'gurmukhi': {'pa'},
            
            'gujarati': {'gu'},
            
            'sinhala': {'si'},
            
            'african': {'sw', 'ha', 'ig', 'ak', 'tw'}
        }
        
        # Define sentence end markers for different script groups
        self.SENTENCE_END_MARKERS = {
            'latin': ['.', '!', '?', ':', ';'],
            'cyrillic': ['.', '!', '?', ':', ';'],
            'devanagari': ['.', '।', '!', '?'],
            'arabic': ['.', '!', '؟', '؛', '،'],
            'cjk': ['。', '！', '？', '：', '；', '，'],
            'thai': ['.', '!', '?', ' '],  # Thai often uses spaces to separate sentences
            'greek': ['.', '!', ';', ':', '·'],
            'hebrew': ['.', '!', '?', ':', ';', '׃'],
            'bengali': ['.', '!', '?', '।'],
            'dravidian': ['.', '!', '?', '।'],
            'gurmukhi': ['.', '!', '?', '।'],
            'gujarati': ['.', '!', '?', '।'],
            'sinhala': ['.', '!', '?', '။'],
            'african': ['.', '!', '?', ':', ';']
        }
        
        # Language-specific section headers and terms
        self.SECTION_HEADERS = self._initialize_section_headers()
        self.FAQ_TERMS = self._initialize_faq_terms()
        self.POLICY_TERMS = self._initialize_policy_terms()
        self.QA_MARKERS = self._initialize_qa_markers()

    def _signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {sig}, initiating graceful shutdown...")
        self.shutdown_requested = True


    def _initialize_section_headers(self) -> Dict[str, List[str]]:
        """Initialize section header terms for different languages"""
        headers = {
            # Default/English
            'en': ['section', 'chapter', 'part'],
            
            # Latin script European languages
            'de': ['abschnitt', 'kapitel', 'teil'],
            'es': ['sección', 'capítulo', 'parte'],
            'fr': ['section', 'chapitre', 'partie'],
            'it': ['sezione', 'capitolo', 'parte'],
            'pt': ['seção', 'capítulo', 'parte'],
            'nl': ['sectie', 'hoofdstuk', 'deel'],
            'pl': ['sekcja', 'rozdział', 'część'],
            'ro': ['secțiune', 'capitol', 'parte'],
            'sv': ['avsnitt', 'kapitel', 'del'],
            'no': ['avsnitt', 'kapittel', 'del'],
            'da': ['afsnit', 'kapitel', 'del'],
            'fi': ['osio', 'luku', 'osa'],
            'hu': ['szakasz', 'fejezet', 'rész'],
            'cs': ['sekce', 'kapitola', 'část'],
            'sk': ['sekcia', 'kapitola', 'časť'],
            'sl': ['razdelek', 'poglavje', 'del'],
            'hr': ['odjeljak', 'poglavlje', 'dio'],
            'bs': ['odjeljak', 'poglavlje', 'dio'],
            'ca': ['secció', 'capítol', 'part'],
            'gl': ['sección', 'capítulo', 'parte'],
            'tr': ['bölüm', 'kısım', 'parça'],
            
            # Cyrillic script languages
            'ru': ['раздел', 'глава', 'часть'],
            'uk': ['розділ', 'глава', 'частина'],
            'bg': ['раздел', 'глава', 'част'],
            'mk': ['дел', 'глава', 'поглавје'],
            'sr-cyr': ['одељак', 'поглавље', 'део'],
            'kk': ['бөлім', 'тарау', 'бөлік'],
            
            # Hindi and other Indic languages
            'hi': ['अनुभाग', 'अध्याय', 'भाग'],
            'mr': ['विभाग', 'अध्याय', 'भाग'],
            'ne': ['खण्ड', 'अध्याय', 'भाग'],
            'bn': ['বিভাগ', 'অধ্যায়', 'অংশ'],
            'pa': ['ਸੈਕਸ਼ਨ', 'ਅਧਿਆਇ', 'ਭਾਗ'],
            'gu': ['વિભાગ', 'અધ્યાય', 'ભાગ'],
            'si': ['කොටස', 'පරිච්ඡේදය', 'කොටස'],
            'ta': ['பிரிவு', 'அத்தியாயம்', 'பகுதி'],
            'te': ['విభాగం', 'అధ్యాయం', 'భాగం'],
            'kn': ['ವಿಭಾಗ', 'ಅಧ್ಯಾಯ', 'ಭಾಗ'],
            'ml': ['വിഭാഗം', 'അധ്യായം', 'ഭാഗം'],
            
            # Middle Eastern languages
            'ar': ['قسم', 'فصل', 'جزء'],
            'fa': ['بخش', 'فصل', 'قسمت'],
            'ur': ['سیکشن', 'باب', 'حصہ'],
            'ps': ['برخه', 'څپرکی', 'برخه'],
            'he': ['חלק', 'פרק', 'סעיף'],
            
            # East Asian languages
            'zh': ['部分', '章节', '节'],
            'zh-tw': ['部分', '章節', '節'],
            'ja': ['セクション', '章', '部'],
            'ko': ['섹션', '장', '부분'],
            
            # Thai
            'th': ['ส่วน', 'บท', 'ตอน'],
            
            # Greek
            'el': ['τμήμα', 'κεφάλαιο', 'μέρος'],
            
            # Indonesian and Malay
            'id': ['bagian', 'bab', 'bagian'],
            'ms': ['bahagian', 'bab', 'bahagian'],
            
            # Vietnamese
            'vi': ['phần', 'chương', 'mục'],
            
            # African languages
            'sw': ['sehemu', 'sura', 'sehemu'],
            'ha': ['sashe', 'babi', 'bangare'],
            'ig': ['nkeji', 'isi', 'akụkụ']
        }
        return headers

    def _initialize_faq_terms(self) -> Dict[str, List[str]]:
        """Initialize FAQ-related terms for different languages"""
        terms = {
            # Default/English
            'en': ['faq', 'frequently asked questions', 'questions and answers', 'q&a'],
            
            # Latin script European languages
            'de': ['faq', 'häufig gestellte fragen', 'fragen und antworten'],
            'es': ['preguntas frecuentes', 'faq', 'preguntas y respuestas'],
            'fr': ['faq', 'foire aux questions', 'questions fréquemment posées'],
            'it': ['faq', 'domande frequenti', 'domande e risposte'],
            'pt': ['faq', 'perguntas frequentes', 'perguntas e respostas'],
            'nl': ['faq', 'veelgestelde vragen', 'vragen en antwoorden'],
            'pl': ['faq', 'często zadawane pytania', 'pytania i odpowiedzi'],
            'ro': ['întrebări frecvente', 'întrebări și răspunsuri'],
            'sv': ['faq', 'vanliga frågor', 'frågor och svar'],
            'no': ['faq', 'ofte stilte spørsmål', 'spørsmål og svar'],
            'da': ['faq', 'ofte stillede spørgsmål', 'spørgsmål og svar'],
            'fi': ['ukk', 'usein kysytyt kysymykset', 'kysymykset ja vastaukset'],
            'hu': ['gyik', 'gyakran ismételt kérdések', 'kérdések és válaszok'],
            'cs': ['často kladené dotazy', 'otázky a odpovědi'],
            'sk': ['často kladené otázky', 'otázky a odpovede'],
            'sl': ['pogosta vprašanja', 'vprašanja in odgovori'],
            'hr': ['često postavljana pitanja', 'pitanja i odgovori'],
            'bs': ['često postavljana pitanja', 'pitanja i odgovori'],
            'tr': ['sss', 'sıkça sorulan sorular', 'sorular ve cevaplar'],
            
            # Cyrillic script languages
            'ru': ['часто задаваемые вопросы', 'вопросы и ответы'],
            'uk': ['часті запитання', 'питання та відповіді'],
            'bg': ['често задавани въпроси', 'въпроси и отговори'],
            'mk': ['често поставувани прашања', 'прашања и одговори'],
            'sr-cyr': ['често постављана питања', 'питања и одговори'],
            
            # Hindi and other Indic languages
            'hi': ['अक्सर पूछे जाने वाले प्रश्न', 'प्रश्न और उत्तर'],
            'mr': ['वारंवार विचारले जाणारे प्रश्न', 'प्रश्न आणि उत्तरे'],
            'ne': ['बारम्बार सोधिने प्रश्नहरू', 'प्रश्न र उत्तरहरू'],
            'bn': ['সচরাচর জিজ্ঞাসিত প্রশ্নাবলী', 'প্রশ্ন ও উত্তর'],
            'pa': ['ਅਕਸਰ ਪੁੱਛੇ ਜਾਣ ਵਾਲੇ ਸਵਾਲ', 'ਸਵਾਲ ਅਤੇ ਜਵਾਬ'],
            'gu': ['વારંવાર પૂછાતા પ્રશ્નો', 'પ્રશ્નો અને જવાબો'],
            'si': ['නිතර අසන ප්‍රශ්න', 'ප්‍රශ්න සහ පිළිතුරු'],
            'ta': ['அடிக்கடி கேட்கப்படும் கேள்விகள்', 'கேள்விகள் மற்றும் பதில்கள்'],
            'te': ['తరచుగా అడిగే ప్రశ్నలు', 'ప్రశ్నలు మరియు సమాధానాలు'],
            'kn': ['ಪದೇ ಪದೇ ಕೇಳಲಾಗುವ ಪ್ರಶ್ನೆಗಳು', 'ಪ್ರಶ್ನೆಗಳು ಮತ್ತು ಉತ್ತರಗಳು'],
            'ml': ['പതിവായി ചോദിക്കുന്ന ചോദ്യങ്ങൾ', 'ചോദ്യങ്ങളും ഉത്തരങ്ങളും'],
            
            # Middle Eastern languages
            'ar': ['الأسئلة الشائعة', 'الأسئلة المتكررة', 'أسئلة وأجوبة'],
            'fa': ['سوالات متداول', 'پرسش و پاسخ'],
            'ur': ['اکثر پوچھے گئے سوالات', 'سوال و جواب'],
            'ps': ['تل پوښتل شوي پوښتنې', 'پوښتنې او ځوابونه'],
            'he': ['שאלות נפוצות', 'שאלות ותשובות'],
            
            # East Asian languages
            'zh': ['常见问题', '常见问答', '问答'],
            'zh-tw': ['常見問題', '常見問答', '問答'],
            'ja': ['よくある質問', 'よくあるご質問', '質問と回答'],
            'ko': ['자주 묻는 질문', '질문과 답변'],
            
            # Thai
            'th': ['คำถามที่พบบ่อย', 'คำถามและคำตอบ'],
            
            # Greek
            'el': ['συχνές ερωτήσεις', 'ερωτήσεις και απαντήσεις'],
            
            # Indonesian and Malay
            'id': ['faq', 'pertanyaan yang sering diajukan', 'tanya jawab'],
            'ms': ['soalan lazim', 'soalan dan jawapan'],
            
            # Vietnamese
            'vi': ['câu hỏi thường gặp', 'hỏi đáp'],
            
            # African languages
            'sw': ['maswali yanayoulizwa mara kwa mara', 'maswali na majibu'],
            'ha': ['tambayoyin da ake yawan yi', 'tambayoyi da amsa'],
            'ig': ['ajụjụ ana-ajụkarị', 'ajụjụ na azịza']
        }
        return terms

    def _initialize_policy_terms(self) -> Dict[str, List[str]]:
        """Initialize policy-related terms for different languages"""
        terms = {
            # Default/English
            'en': ['policy', 'terms', 'conditions', 'agreement', 'privacy', 'legal'],
            
            # Latin script European languages
            'de': ['richtlinie', 'bedingungen', 'vereinbarung', 'datenschutz', 'rechtlich'],
            'es': ['política', 'términos', 'condiciones', 'acuerdo', 'privacidad', 'legal'],
            'fr': ['politique', 'conditions', 'accord', 'confidentialité', 'légal'],
            'it': ['politica', 'termini', 'condizioni', 'accordo', 'privacy', 'legale'],
            'pt': ['política', 'termos', 'condições', 'acordo', 'privacidade', 'legal'],
            'nl': ['beleid', 'voorwaarden', 'overeenkomst', 'privacy', 'juridisch'],
            'pl': ['polityka', 'warunki', 'umowa', 'prywatność', 'prawny'],
            'ro': ['politică', 'termeni', 'condiții', 'acord', 'confidențialitate', 'legal'],
            'sv': ['policy', 'villkor', 'avtal', 'integritet', 'juridiskt'],
            'no': ['retningslinjer', 'vilkår', 'avtale', 'personvern', 'juridisk'],
            'da': ['politik', 'vilkår', 'betingelser', 'aftale', 'privatlivspolitik', 'juridisk'],
            'fi': ['käytäntö', 'ehdot', 'sopimus', 'yksityisyys', 'laillinen'],
            'hu': ['szabályzat', 'feltételek', 'megállapodás', 'adatvédelem', 'jogi'],
            'cs': ['zásady', 'podmínky', 'smlouva', 'soukromí', 'právní'],
            'sk': ['zásady', 'podmienky', 'dohoda', 'súkromie', 'právne'],
            'sl': ['politika', 'pogoji', 'sporazum', 'zasebnost', 'pravno'],
            'hr': ['politika', 'uvjeti', 'ugovor', 'privatnost', 'pravno'],
            'bs': ['politika', 'uslovi', 'sporazum', 'privatnost', 'pravno'],
            'tr': ['politika', 'şartlar', 'koşullar', 'anlaşma', 'gizlilik', 'yasal'],
            
            # Cyrillic script languages
            'ru': ['политика', 'условия', 'соглашение', 'конфиденциальность', 'правовой'],
            'uk': ['політика', 'умови', 'угода', 'конфіденційність', 'правовий'],
            'bg': ['политика', 'условия', 'споразумение', 'поверителност', 'правен'],
            'mk': ['политика', 'услови', 'договор', 'приватност', 'правно'],
            'sr-cyr': ['политика', 'услови', 'уговор', 'приватност', 'правно'],
            
            # Hindi and other Indic languages
            'hi': ['नीति', 'शर्तें', 'समझौता', 'गोपनीयता', 'कानूनी'],
            'mr': ['धोरण', 'अटी', 'करार', 'गोपनीयता', 'कायदेशीर'],
            'ne': ['नीति', 'सर्तहरू', 'सम्झौता', 'गोपनीयता', 'कानूनी'],
            'bn': ['নীতি', 'শর্তাবলী', 'চুক্তি', 'গোপনীয়তা', 'আইনি'],
            'pa': ['ਨੀਤੀ', 'ਸ਼ਰਤਾਂ', 'ਸਮਝੌਤਾ', 'ਗੋਪਨੀਯਤਾ', 'ਕਾਨੂੰਨੀ'],
            'gu': ['નીતિ', 'શરતો', 'કરાર', 'ગોપનીયતા', 'કાનૂની'],
            'si': ['ප්‍රතිපත්තිය', 'කොන්දේසි', 'ගිවිසුම', 'පුද්ගලිකත්වය', 'නීතිමය'],
            'ta': ['கொள்கை', 'விதிமுறைகள்', 'ஒப்பந்தம்', 'தனியுரிமை', 'சட்டப்பூர்வ'],
            'te': ['విధానం', 'నిబంధనలు', 'ఒప్పందం', 'గోప్యత', 'చట్టపరమైన'],
            'kn': ['ನೀತಿ', 'ನಿಯಮಗಳು', 'ಒಪ್ಪಂದ', 'ಗೌಪ್ಯತೆ', 'ಕಾನೂನು'],
            'ml': ['നയം', 'നിബന്ധനകൾ', 'കരാർ', 'സ്വകാര്യത', 'നിയമപരമായ'],
            
            # Middle Eastern languages
            'ar': ['سياسة', 'شروط', 'اتفاقية', 'خصوصية', 'قانوني'],
            'fa': ['سیاست', 'شرایط', 'توافق', 'حریم خصوصی', 'قانونی'],
            'ur': ['پالیسی', 'شرائط', 'معاہدہ', 'رازداری', 'قانونی'],
            'ps': ['تګلاره', 'شرایط', 'تړون', 'محرمیت', 'قانوني'],
            'he': ['מדיניות', 'תנאים', 'הסכם', 'פרטיות', 'משפטי'],
            
            # East Asian languages
            'zh': ['政策', '条款', '协议', '隐私', '法律'],
            'zh-tw': ['政策', '條款', '協議', '隱私', '法律'],
            'ja': ['ポリシー', '規約', '契約', 'プライバシー', '法的'],
            'ko': ['정책', '약관', '계약', '개인정보', '법적'],
            
            # Thai
            'th': ['นโยบาย', 'เงื่อนไข', 'ข้อตกลง', 'ความเป็นส่วนตัว', 'ทางกฎหมาย'],
            
            # Greek
            'el': ['πολιτική', 'όροι', 'συμφωνία', 'απόρρητο', 'νομικό'],
            
            # Indonesian and Malay
            'id': ['kebijakan', 'syarat', 'ketentuan', 'perjanjian', 'privasi', 'hukum'],
            'ms': ['dasar', 'terma', 'syarat', 'perjanjian', 'privasi', 'undang-undang'],
            
            # Vietnamese
            'vi': ['chính sách', 'điều khoản', 'thỏa thuận', 'quyền riêng tư', 'pháp lý'],
            
            # African languages
            'sw': ['sera', 'masharti', 'makubaliano', 'faragha', 'kisheria'],
            'ha': ['manufa', 'sharuɗɗa', 'yarjejeniya', 'sirri', 'na doka'],
            'ig': ['iwu', 'usoro', 'nkwekọrịta', 'nzuzo', 'iwu']
        }
        return terms

    def _initialize_qa_markers(self) -> Dict[str, List[Tuple[str, str]]]:
        """Initialize Q&A markers for different languages"""
        markers = {
            # Default/English
            'en': [('Q', 'A'), ('Question', 'Answer')],
            
            # Latin script European languages
            'de': [('F', 'A'), ('Frage', 'Antwort')],
            'es': [('P', 'R'), ('Pregunta', 'Respuesta')],
            'fr': [('Q', 'R'), ('Question', 'Réponse')],
            'it': [('D', 'R'), ('Domanda', 'Risposta')],
            'pt': [('P', 'R'), ('Pergunta', 'Resposta')],
            'nl': [('V', 'A'), ('Vraag', 'Antwoord')],
            'pl': [('P', 'O'), ('Pytanie', 'Odpowiedź')],
            'ro': [('Î', 'R'), ('Întrebare', 'Răspuns')],
            'sv': [('F', 'S'), ('Fråga', 'Svar')],
            'no': [('S', 'S'), ('Spørsmål', 'Svar')],
            'da': [('S', 'S'), ('Spørgsmål', 'Svar')],
            'fi': [('K', 'V'), ('Kysymys', 'Vastaus')],
            'hu': [('K', 'V'), ('Kérdés', 'Válasz')],
            'cs': [('O', 'O'), ('Otázka', 'Odpověď')],
            'sk': [('O', 'O'), ('Otázka', 'Odpoveď')],
            'sl': [('V', 'O'), ('Vprašanje', 'Odgovor')],
            'hr': [('P', 'O'), ('Pitanje', 'Odgovor')],
            'bs': [('P', 'O'), ('Pitanje', 'Odgovor')],
            'tr': [('S', 'C'), ('Soru', 'Cevap')],
            
            # Cyrillic script languages
            'ru': [('В', 'О'), ('Вопрос', 'Ответ')],
            'uk': [('П', 'В'), ('Питання', 'Відповідь')],
            'bg': [('В', 'О'), ('Въпрос', 'Отговор')],
            'mk': [('П', 'О'), ('Прашање', 'Одговор')],
            'sr-cyr': [('П', 'О'), ('Питање', 'Одговор')],
            'kk': [('С', 'Ж'), ('Сұрақ', 'Жауап')],
            
            # Hindi and other Indic languages
            'hi': [('प्र', 'उ'), ('प्रश्न', 'उत्तर'), ('सवाल', 'जवाब')],
            'mr': [('प्र', 'उ'), ('प्रश्न', 'उत्तर')],
            'ne': [('प्र', 'उ'), ('प्रश्न', 'उत्तर')],
            'bn': [('প্র', 'উ'), ('প্রশ্ন', 'উত্তর')],
            'pa': [('ਸ', 'ਜ'), ('ਸਵਾਲ', 'ਜਵਾਬ')],
            'gu': [('પ્ર', 'ઉ'), ('પ્રશ્ન', 'ઉત્તર')],
            'si': [('ප්‍ර', 'පි'), ('ප්‍රශ්නය', 'පිළිතුර')],
            'ta': [('கே', 'ப'), ('கேள்வி', 'பதில்')],
            'te': [('ప్ర', 'జ'), ('ప్రశ్న', 'జవాబు')],
            'kn': [('ಪ್ರ', 'ಉ'), ('ಪ್ರಶ್ನೆ', 'ಉತ್ತರ')],
            'ml': [('ചോ', 'ഉ'), ('ചോദ്യം', 'ഉത്തരം')],
            
            # Middle Eastern languages
            'ar': [('س', 'ج'), ('سؤال', 'جواب')],
            'fa': [('س', 'ج'), ('سوال', 'جواب')],
            'ur': [('س', 'ج'), ('سوال', 'جواب')],
            'ps': [('پ', 'ځ'), ('پوښتنه', 'ځواب')],
            'he': [('ש', 'ת'), ('שאלה', 'תשובה')],
            
            # East Asian languages
            'zh': [('问', '答'), ('问题', '回答'), ('Q', 'A')],
            'zh-tw': [('問', '答'), ('問題', '回答'), ('Q', 'A')],
            'ja': [('質問', '回答'), ('問', '答'), ('Q', 'A')],
            'ko': [('질문', '답변'), ('문', '답'), ('Q', 'A')],
            
            # Thai
            'th': [('คำถาม', 'คำตอบ'), ('ถาม', 'ตอบ'), ('Q', 'A')],
            
            # Greek
            'el': [('Ε', 'Α'), ('Ερώτηση', 'Απάντηση')],
            
            # Indonesian and Malay
            'id': [('T', 'J'), ('Tanya', 'Jawab')],
            'ms': [('S', 'J'), ('Soalan', 'Jawapan')],
            
            # Vietnamese
            'vi': [('H', 'Đ'), ('Hỏi', 'Đáp')],
            
            # African languages
            'sw': [('S', 'J'), ('Swali', 'Jibu')],
            'ha': [('T', 'A'), ('Tambaya', 'Amsa')],
            'ig': [('A', 'A'), ('Ajụjụ', 'Azịza')]
        }
        return markers

    def get_script_group(self, language: str) -> str:
        """
        Get the script group for a language.
        
        Args:
            language: Language code
            
        Returns:
            Script group name
        """
        for group, langs in self.SCRIPT_GROUPS.items():
            if language in langs:
                return group
        return 'en'  # Default to Latin script

    def get_sentence_end_markers(self, language: str) -> List[str]:
        """
        Get sentence end markers for a language.
        
        Args:
            language: Language code
            
        Returns:
            List of end markers
        """
        script_group = self.get_script_group(language)
        return self.SENTENCE_END_MARKERS.get(script_group, self.SENTENCE_END_MARKERS['latin'])

    def get_section_headers(self, language: str) -> List[str]:
        """
        Get section header terms for a language.
        
        Args:
            language: Language code
            
        Returns:
            List of section header terms
        """
        return self.SECTION_HEADERS.get(language, self.SECTION_HEADERS['en'])

    def get_faq_terms(self, language: str) -> List[str]:
        """
        Get FAQ-related terms for a language.
        
        Args:
            language: Language code
            
        Returns:
            List of FAQ terms
        """
        return self.FAQ_TERMS.get(language, self.FAQ_TERMS['en'])

    def get_policy_terms(self, language: str) -> List[str]:
        """
        Get policy-related terms for a language.
        
        Args:
            language: Language code
            
        Returns:
            List of policy terms
        """
        return self.POLICY_TERMS.get(language, self.POLICY_TERMS['en'])

    def get_qa_markers(self, language: str) -> List[Tuple[str, str]]:
        """
        Get Q&A markers for a language.
        
        Args:
            language: Language code
            
        Returns:
            List of (question_marker, answer_marker) tuples
        """
        return self.QA_MARKERS.get(language, self.QA_MARKERS['en'])

    def detect_best_language(self,text):
   
        if len(self.libraryDetector.available_libraries) > 0:
            lib_signals = self.libraryDetector._detect_with_libraries(text)
            logger.info(f" libraryDetector {lib_signals} ")
            
            if lib_signals:
                # Get best library result
                best_signal = max(lib_signals, key=lambda x: x[2])
                _, lang, conf = best_signal
                if conf > 0.5:  # If reasonably confident
                    return lang
                    
        # 3. Fall back to combined approach
        return 'en'



    def detect_document_type(self, text: str, language: str) -> str:
        """
        Detect document type based on content and language-specific indicators.
        
        Args:
            text: Document text
            language: Language code
            
        Returns:
            Document type: 'faq', 'policy', 'history', 'general'
        """
        text_lower = text.lower()
        
        # Check for FAQ indicators in the appropriate language
        faq_terms = self.get_faq_terms(language)
        logger.info(f" faq_terms : {faq_terms} , text_lower : {text_lower}")
        if any(term in text_lower for term in faq_terms):
            return 'faq'
        
        # Count question marks or language-specific question indicators
        script_group = self.get_script_group(language)
        if script_group == 'cjk':
            question_count = len(re.findall(r'[?？]', text))
        elif script_group == 'arabic':
            question_count = len(re.findall(r'[?؟]', text))
        else:
            question_count = len(re.findall(r'\?', text))
            
        if question_count > 3:
            return 'faq'
        
        # Check for policy document indicators
        policy_terms = self.get_policy_terms(language)
        if any(term in text_lower for term in policy_terms):
            return 'policy'
        
        # Check for numbered sections which are common in policies
        # Adapt regex for script groups that use different numbering systems
        if script_group == 'cjk':
            # Using Chinese/Japanese numbering
            numbered_pattern = r'[一二三四五六七八九十][\s、．\.]'
        elif script_group in ['devanagari', 'bengali', 'gurmukhi', 'gujarati', 'dravidian']:
            # Using Indic numbering or Latin numbers
            numbered_pattern = r'(?:\d+|[१२३४५६७८९०]+)[\s\.\-\)]'
        elif script_group == 'arabic':
            # Arabic numerals or Arabic-Indic numerals
            numbered_pattern = r'(?:\d+|[١٢٣٤٥٦٧٨٩٠]+)[\s\.\-\)]'
        else:
            # Default Latin numbering
            numbered_pattern = r'\d+[\s\.\-\)]'
            
        numbered_sections = re.findall(numbered_pattern, text)
        if len(numbered_sections) > 3:
            return 'policy'
        
        # Default to general content
        return 'general'

    def split_into_sentences(self, text: str, language: str) -> List[str]:
        """
        Split text into sentences with language-specific rules.
        
        Args:
            text: Text to split
            language: Language code
            
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
        script_group = self.get_script_group(language)
        end_markers = self.get_sentence_end_markers(language)
        
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

    def identify_document_sections(self, text: str, language: str, 
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
        
        script_group = self.get_script_group(language)
        
        # PRIORITY 1: Check for manual dividers first
        manual_sections = self._identify_manual_dividers(text, min_divider_length)
        if manual_sections:
            print(f"Found {len(manual_sections)} sections using manual dividers")
            
            # Check if any manual sections are too large and need further subdivision
            final_manual_sections = []
            for title, content in manual_sections:
                if len(content) > max_section_length:
                    print(f"Section '{title}' is too large ({len(content)} chars), subdividing...")
                    # Subdivide large manual sections
                    sub_sections = self._subdivide_large_section(title, content, language, script_group, 
                                                            max_section_length, min_section_length)
                    final_manual_sections.extend(sub_sections)
                else:
                    final_manual_sections.append((title, content))
            
            # Add overlapping content between manually divided sections
            overlapped_sections = self._add_section_overlaps(final_manual_sections, text, language, overlap_sentences)
            return self._ensure_complete_coverage(overlapped_sections, text, language)
        
        # PRIORITY 2: Try to identify natural sections using existing logic
        sections = self._identify_natural_sections(text, language, script_group)
        
        # PRIORITY 3: If no natural sections found or sections are too long, create artificial sections
        if not sections or any(len(content) > max_section_length for _, content in sections):
            sections = self._create_chunked_sections(text, language, script_group, 
                                                max_section_length, min_section_length)
        
        # Add overlapping content between sections
        overlapped_sections = self._add_section_overlaps(sections, text, language, 
                                                    overlap_sentences)
        
        # Ensure complete text coverage
        final_sections = self._ensure_complete_coverage(overlapped_sections, text, language)
        
        return final_sections

    def _identify_manual_dividers(self, text: str, min_divider_length: int = 5) -> List[Tuple[str, str]]:
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
                            title = self._extract_section_title_from_content(content_before, section_num)
                            sections.append((title, content_before))
                            section_num += 1
                    
                    # Update start position for next section
                    start_pos = divider_end
                
                # Handle content after the last divider
                if start_pos < len(text):
                    remaining_content = text[start_pos:].strip()
                    if remaining_content and len(remaining_content) >= 10:
                        title = self._extract_section_title_from_content(remaining_content, section_num)
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
                    title = self._extract_section_title_from_content(content, 1)
                    sections.append((title, content))
            
            # Content between dividers
            for i in range(len(all_dividers)):
                start_pos = all_dividers[i].end()
                end_pos = all_dividers[i + 1].start() if i + 1 < len(all_dividers) else len(text)
                
                content = text[start_pos:end_pos].strip()
                if content and len(content) >= 10:
                    title = self._extract_section_title_from_content(content, len(sections) + 1)
                    sections.append((title, content))
            
            if len(sections) > 0:
                return sections
        
        return []  # No manual dividers found

    def _extract_section_title_from_content(self, content: str, section_num: int) -> str:
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
                if self._looks_like_title(line):
                    return line[:100]  # Limit title length
        
        # Try to find a line that's shorter and might be a header
        for line in lines[:5]:
            line = line.strip()
            if line and 5 < len(line) < 80 and not line.endswith(('.', '。', '।', '؟', '?', '!')):
                return line
        
        # Extract key words from first sentence for auto-title
        first_sentence = self._get_first_sentence(content)
        if first_sentence and 10 < len(first_sentence) < 100:
            return first_sentence
        
        # Fallback to generic title
        return f"Section {section_num}"

    def _looks_like_title(self, line: str) -> bool:
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

    def _get_first_sentence(self, text: str) -> str:
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

    def _identify_natural_sections(self, text: str, language: str, script_group: str) -> List[Tuple[str, str]]:
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
        section_terms = self.get_section_headers(language)
        sections = self._find_sections_by_headers(text, section_terms, script_group)
        
        if sections:
            return sections
        
        # Try numbered sections
        numbered_sections = self._find_numbered_sections(text, script_group)
        if numbered_sections:
            return numbered_sections
        
        # Try other visual separators (but not the manual dividers we already checked)
        separator_sections = self._find_other_separator_sections(text)
        if separator_sections:
            return separator_sections
        
        return []

    def _find_sections_by_headers(self, text: str, section_terms: List[str], 
                                script_group: str) -> List[Tuple[str, str]]:
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

    def _find_numbered_sections(self, text: str, script_group: str) -> List[Tuple[str, str]]:
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
                        title = match.group(1).strip()
                    
                    # Find content
                    content_start = match.end()
                    content_end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
                    content = text[content_start:content_end].strip()
                    
                    if content:
                        sections.append((title, content))
                
                if sections:
                    return sections
        
        return []

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

    def _create_chunked_sections(self, text: str, language: str, script_group: str,
                            max_section_length: int, min_section_length: int) -> List[Tuple[str, str]]:
        """Create artificial sections by chunking the text intelligently."""
        if len(text) <= max_section_length:
            return [("General", text)]
        
        # Get sentence boundaries based on script group
        sentence_endings = self._get_sentence_endings(script_group)
        
        sections = []
        current_pos = 0
        section_num = 1
        
        while current_pos < len(text):
            # Find a good breaking point within max_section_length
            end_pos = min(current_pos + max_section_length, len(text))
            
            # First, check if we're in the middle of a list - if so, find the end
            best_break = self._find_safe_break_point(text, current_pos, end_pos, 
                                                min_section_length, sentence_endings, script_group)
            
            # If we still don't have a good break and we're not at the end, extend to complete the list
            if best_break == end_pos and end_pos < len(text):
                list_end = self._find_list_end(text, end_pos, script_group)
                if list_end > end_pos and list_end - current_pos < max_section_length * 1.5:  # Allow 50% extension for lists
                    best_break = list_end
            
            section_content = text[current_pos:best_break].strip()
            if section_content:
                # Generate meaningful title from first line or sentences
                title = self._extract_section_title_from_content(section_content, section_num)
                sections.append((title, section_content))
                section_num += 1
            
            current_pos = best_break
        
        return sections

    def _get_sentence_endings(self, script_group: str) -> str:
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

    def _subdivide_large_section(self, parent_title: str, content: str, language: str, script_group: str,
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
        natural_subsections = self._find_natural_subsections_in_content(content, language, script_group)
        
        if natural_subsections and len(natural_subsections) > 1:
            # Check if natural subsections are reasonable sized
            reasonable_subsections = []
            for sub_title, sub_content in natural_subsections:
                if len(sub_content) > max_section_length:
                    # Even natural subsections are too big, chunk them further
                    chunked = self._create_chunked_sections_with_prefix(
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
        return self._create_chunked_sections_with_prefix(
            content, parent_title, language, script_group, max_section_length, min_section_length
        )

    def _find_natural_subsections_in_content(self, content: str, language: str, script_group: str) -> List[Tuple[str, str]]:
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
        numbered_patterns = self._get_subsection_number_patterns(script_group)
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
                        title = self._extract_section_title_from_content(section_content, len(sections) + 1)
                        sections.append((title, section_content))
                        current_section = []
                        current_length = 0
            
            if len(sections) > 1:
                return sections
        
        return []

    def _get_subsection_number_patterns(self, script_group: str) -> List[str]:
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

    def _create_chunked_sections_with_prefix(self, text: str, parent_title: str, language: str, script_group: str,
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
        sentence_endings = self._get_sentence_endings(script_group)
        
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
                    part_word = self._get_part_word(language)
                    title = f"{parent_title} - {part_word} {chunk_num}"
                
                sections.append((title, section_content))
                chunk_num += 1
            
            current_pos = best_break
        
        return sections

    def _find_safe_break_point(self, text: str, current_pos: int, end_pos: int, 
                            min_section_length: int, sentence_endings: str, script_group: str) -> int:
        """
        Find a safe break point that doesn't split ordered lists, bullet points, or numbered items.
        
        Args:
            text: Full text
            current_pos: Starting position
            end_pos: Desired ending position
            min_section_length: Minimum section length
            sentence_endings: Language-specific sentence endings
            script_group: Script group for language
            
        Returns:
            Safe break position
        """
        # Check if we're in the middle of a list at end_pos
        if self._is_in_list_context(text, end_pos):
            # Try to find the end of the current list
            list_end = self._find_list_end(text, end_pos, script_group)
            # If list end is reasonable, use it
            if list_end <= end_pos + 500:  # Don't extend too far
                return list_end
        
        # Try to break at sentence boundary, but avoid breaking lists
        best_break = end_pos
        for i in range(end_pos - 1, max(current_pos + min_section_length, end_pos - 200), -1):
            if text[i] in sentence_endings and i > current_pos + min_section_length:
                # Check if this sentence ending is safe (not in middle of a list)
                if not self._is_in_list_context(text, i):
                    best_break = i + 1
                    break
        
        # If no good sentence break found, try paragraph break
        if best_break == end_pos and end_pos < len(text):
            for i in range(end_pos - 1, max(current_pos + min_section_length, end_pos - 100), -1):
                if text[i:i+2] == '\n\n':
                    # Double check this isn't breaking a list
                    if not self._is_in_list_context(text, i):
                        best_break = i + 2
                        break
        
        return best_break

    def _is_in_list_context(self, text: str, position: int) -> bool:
        """
        Check if the given position is within a list context (numbered, bulleted, etc.).
        
        Args:
            text: Full text
            position: Position to check
            
        Returns:
            True if position is within a list
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
            if self._is_list_item(line):
                list_pattern_count += 1
        
        # If we found multiple list items recently, we're likely in a list
        return list_pattern_count >= 2

    def _is_list_item(self, line: str) -> bool:
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

    def _find_list_end(self, text: str, start_pos: int, script_group: str) -> int:
        """
        Find the end of a list starting from the given position.
        
        Args:
            text: Full text
            start_pos: Position to start looking for list end
            script_group: Script group for language-specific patterns
            
        Returns:
            Position where the list ends
        """
        lines = text[start_pos:].split('\n')
        
        list_end_pos = start_pos
        consecutive_non_list_lines = 0
        in_list = False
        
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            
            # Check if this line is a list item
            if self._is_list_item(line_stripped):
                in_list = True
                consecutive_non_list_lines = 0
                # Update end position to include this line
                list_end_pos = start_pos + len('\n'.join(lines[:i+1]))
            
            elif in_list:
                # We were in a list, check if this line continues the list context
                if self._is_list_continuation(line_stripped):
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

    def _is_list_continuation(self, line: str) -> bool:
        """
        Check if a line is a continuation of a list item (like indented content).
        
        Args:
            line: Line to check
            
        Returns:
            True if line continues a list item
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
        """Get the word for 'Part' in the given language."""
        part_words = {
            'en': 'Part', 'es': 'Parte', 'fr': 'Partie', 'de': 'Teil',
            'it': 'Parte', 'pt': 'Parte', 'ru': 'Часть', 'zh': '部分',
            'ja': 'パート', 'ko': '부분', 'ar': 'جزء', 'hi': 'भाग',
            'th': 'ส่วน', 'he': 'חלק', 'bn': 'অংশ', 'ta': 'பகுதி'
        }
        return part_words.get(language, 'Part')
        """Get the word for 'Section' in the given language."""
        section_words = {
            'en': 'Section', 'es': 'Sección', 'fr': 'Section', 'de': 'Abschnitt',
            'it': 'Sezione', 'pt': 'Seção', 'ru': 'Раздел', 'zh': '章节',
            'ja': 'セクション', 'ko': '섹션', 'ar': 'قسم', 'hi': 'खंड',
            'th': 'ส่วน', 'he': 'סעיף', 'bn': 'বিভাগ', 'ta': 'பிரிவு'
        }
        return section_words.get(language, 'Section')

    def _add_section_overlaps(self, sections: List[Tuple[str, str]], full_text: str,
                            language: str, overlap_sentences: int) -> List[Tuple[str, str]]:
        """Add overlapping content between adjacent sections."""
        if len(sections) <= 1 or overlap_sentences <= 0:
            return sections
        
        script_group = self.get_script_group(language)
        sentence_endings = self._get_sentence_endings(script_group)
        
        overlapped_sections = []
        
        for i, (title, content) in enumerate(sections):
            extended_content = content
            
            # Add overlap from previous section (suffix)
            if i > 0:
                prev_content = sections[i-1][1]
                prev_sentences = self._split_sentences(prev_content, sentence_endings)
                if len(prev_sentences) >= overlap_sentences:
                    overlap_text = ' '.join(prev_sentences[-overlap_sentences:])
                    extended_content = overlap_text + '\n\n' + extended_content
            
            # Add overlap from next section (prefix)
            if i < len(sections) - 1:
                next_content = sections[i+1][1]
                next_sentences = self._split_sentences(next_content, sentence_endings)
                if len(next_sentences) >= overlap_sentences:
                    overlap_text = ' '.join(next_sentences[:overlap_sentences])
                    extended_content = extended_content + '\n\n' + overlap_text
            
            overlapped_sections.append((title, extended_content))
        
        return overlapped_sections

    def _split_sentences(self, text: str, sentence_endings: str) -> List[str]:
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

    def _get_sentence_endings(self, script_group: str) -> str:
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

    def _get_part_word(self, language: str) -> str:
        """Get the word for 'Part' in the given language."""
        part_words = {
            'en': 'Part', 'es': 'Parte', 'fr': 'Partie', 'de': 'Teil',
            'it': 'Parte', 'pt': 'Parte', 'ru': 'Часть', 'zh': '部分',
            'ja': 'パート', 'ko': '부분', 'ar': 'جزء', 'hi': 'भाग',
            'th': 'ส่วน', 'he': 'חלק', 'bn': 'অংশ', 'ta': 'பகுதி'
        }
        return part_words.get(language, 'Part')
        """Get the word for 'Section' in the given language."""
        section_words = {
            'en': 'Section', 'es': 'Sección', 'fr': 'Section', 'de': 'Abschnitt',
            'it': 'Sezione', 'pt': 'Seção', 'ru': 'Раздел', 'zh': '章节',
            'ja': 'セクション', 'ko': '섹션', 'ar': 'قسم', 'hi': 'खंड',
            'th': 'ส่วน', 'he': 'סעיף', 'bn': 'বিভাগ', 'ta': 'பிரிவு'
        }
        return section_words.get(language, 'Section')

    def _add_section_overlaps(self, sections: List[Tuple[str, str]], full_text: str,
                            language: str, overlap_sentences: int) -> List[Tuple[str, str]]:
        """Add overlapping content between adjacent sections."""
        if len(sections) <= 1 or overlap_sentences <= 0:
            return sections
        
        script_group = self.get_script_group(language)
        sentence_endings = self._get_sentence_endings(script_group)
        
        overlapped_sections = []
        
        for i, (title, content) in enumerate(sections):
            extended_content = content
            
            # Add overlap from previous section (suffix)
            if i > 0:
                prev_content = sections[i-1][1]
                prev_sentences = self._split_sentences(prev_content, sentence_endings)
                if len(prev_sentences) >= overlap_sentences:
                    overlap_text = ' '.join(prev_sentences[-overlap_sentences:])
                    extended_content = overlap_text + '\n\n' + extended_content
            
            # Add overlap from next section (prefix)
            if i < len(sections) - 1:
                next_content = sections[i+1][1]
                next_sentences = self._split_sentences(next_content, sentence_endings)
                if len(next_sentences) >= overlap_sentences:
                    overlap_text = ' '.join(next_sentences[:overlap_sentences])
                    extended_content = extended_content + '\n\n' + overlap_text
            
            overlapped_sections.append((title, extended_content))
        
        return overlapped_sections


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


    def _identify_natural_sections(self, text: str, language: str, script_group: str) -> List[Tuple[str, str]]:
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
        section_terms = self.get_section_headers(language)
        sections = self._find_sections_by_headers(text, section_terms, script_group)
        
        if sections:
            return sections
        
        # Try numbered sections
        numbered_sections = self._find_numbered_sections(text, script_group)
        if numbered_sections:
            return numbered_sections
        
        # Try visual separators
        separator_sections = self._find_separator_sections(text)
        if separator_sections:
            return separator_sections
        
        return []

    def _find_sections_by_headers(self, text: str, section_terms: List[str], 
                                script_group: str) -> List[Tuple[str, str]]:
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

    def _find_numbered_sections(self, text: str, script_group: str) -> List[Tuple[str, str]]:
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
                        title = match.group(1).strip()
                    
                    # Find content
                    content_start = match.end()
                    content_end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
                    content = text[content_start:content_end].strip()
                    
                    if content:
                        sections.append((title, content))
                
                if sections:
                    return sections
        
        return []

    def _find_separator_sections(self, text: str) -> List[Tuple[str, str]]:
        """Find sections separated by visual separators."""
        separator_patterns = [
            r'\n\s*[-=*_]{3,}\s*\n',  # Horizontal rules
            r'\n\s*[•·]{3,}\s*\n',    # Bullet separators
            r'\n\s*\n\s*\n',          # Multiple blank lines
            r'\n\s*[~]{3,}\s*\n'      # Tilde separators
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

    def _create_chunked_sections(self, text: str, language: str, script_group: str,
                            max_section_length: int, min_section_length: int) -> List[Tuple[str, str]]:
        """Create artificial sections by chunking the text intelligently."""
        if len(text) <= max_section_length:
            return [("General", text)]
        
        # Get sentence boundaries based on script group
        sentence_endings = self._get_sentence_endings(script_group)
        
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
                title = self._generate_section_title(section_content, section_num, language)
                sections.append((title, section_content))
                section_num += 1
            
            current_pos = best_break
        
        return sections


    def _generate_section_title(self, content: str, section_num: int, language: str) -> str:
        """Generate a meaningful title for a section."""
        lines = content.split('\n')
        first_line = lines[0].strip()
        
        # If first line is short and looks like a title, use it
        if len(first_line) < 80 and not first_line.endswith(('.', '。', '।', '؟', '?', '!')):
            return first_line
        
        # Try to extract key phrases from first sentence
        first_sentence = first_line
        sentence_endings = self._get_sentence_endings(self.get_script_group(language))
        
        for ending in sentence_endings:
            if ending in first_line:
                first_sentence = first_line[:first_line.index(ending) + 1]
                break
        
        # If sentence is reasonable length for a title
        if 10 < len(first_sentence) < 80:
            return first_sentence.strip()
        
        # Fallback to generic title
        section_word = self._get_section_word(language)
        return f"{section_word} {section_num}"

    def _get_section_word(self, language: str) -> str:
        """Get the word for 'Section' in the given language."""
        section_words = {
            'en': 'Section', 'es': 'Sección', 'fr': 'Section', 'de': 'Abschnitt',
            'it': 'Sezione', 'pt': 'Seção', 'ru': 'Раздел', 'zh': '章节',
            'ja': 'セクション', 'ko': '섹션', 'ar': 'قسم', 'hi': 'खंड',
            'th': 'ส่วน', 'he': 'סעיף', 'bn': 'বিভাগ', 'ta': 'பிரிவு'
        }
        return section_words.get(language, 'Section')


    def _split_sentences(self, text: str, sentence_endings: str) -> List[str]:
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

  
    def extract_qa_pairs(self, text: str, language: str) -> List[Tuple[str, str]]:
        """
        Extract question-answer pairs with language-specific patterns.
        
        Args:
            text: Document text
            language: Language code
            
        Returns:
            List of (question, answer) tuples
        """
        qa_pairs = []
        
        # Try explicit Q&A format with language-specific markers
        qa_markers = self.get_qa_markers(language)
        
        for q_marker, a_marker in qa_markers:
            # Try variations with different separators
            for separator in [':', '.', ' ']:
                q_pattern = f"{q_marker}{separator}"
                a_pattern = f"{a_marker}{separator}"
                
                # Pattern for "Q: question\nA: answer"
                pattern = rf'(?:^|\n)\s*{re.escape(q_pattern)}\s*(.*?)(?:\n|\r\n?)\s*{re.escape(a_pattern)}\s*(.*?)(?=\n\s*{re.escape(q_pattern)}|\Z)'
                
                matches = re.finditer(pattern, text, re.DOTALL | re.IGNORECASE)
                for match in matches:
                    question = match.group(1).strip()
                    answer = match.group(2).strip()
                    
                    if question and answer:
                        qa_pairs.append((question, answer))
        
        # If no explicit Q&A pairs found, try numbered format
        if not qa_pairs:
            # Adapt pattern based on script group
            script_group = self.get_script_group(language)
            
            if script_group == 'cjk':
                # For CJK, look for numbered questions with CJK or Arabic numerals
                pattern = r'(?:^|\n)\s*(?:\d+|[一二三四五六七八九十]+)[\.．、]?\s*([^？？\n]*[？？])\s*(.*?)(?=\n\s*(?:\d+|[一二三四五六七八九十]+)[\.．、]|\Z)'
            elif script_group in ['devanagari', 'bengali', 'dravidian', 'gurmukhi', 'gujarati']:
                # For Indic scripts
                pattern = r'(?:^|\n)\s*(?:\d+|[१२३४५६७८९०]+)[\.।]?\s*([^\n]*\?)\s*(.*?)(?=\n\s*(?:\d+|[१२३४५६७८९०]+)[\.।]|\Z)'
            elif script_group == 'arabic':
                # For Arabic script
                pattern = r'(?:^|\n)\s*(?:\d+|[١٢٣٤٥٦٧٨٩٠]+)[\.،]?\s*([^\n]*[\?؟])\s*(.*?)(?=\n\s*(?:\d+|[١٢٣٤٥٦٧٨٩٠]+)[\.،]|\Z)'
            else:
                # Default pattern for Latin script and others
                pattern = r'(?:^|\n)\s*\d+\.?\s*([^\n]*\??)\s*(.*?)(?=\n\s*\d+\.|\Z)'
                
            matches = re.finditer(pattern, text, re.DOTALL)
            for match in matches:
                question = match.group(1).strip()
                answer = match.group(2).strip()
                
                # Verify it looks like a question (has ? or ends with question mark in appropriate script)
                is_question = False
                if '?' in question:
                    is_question = True
                elif script_group == 'cjk' and ('？' in question):
                    is_question = True
                elif script_group == 'arabic' and ('؟' in question):
                    is_question = True
                
                if is_question and question and answer:
                    qa_pairs.append((question, answer))
        
        return qa_pairs

    def extract_chunks(self, text: str, language: str) -> List[Chunk]:
        """
        Extract semantic chunks from text with language-aware processing.
        Ensures ALL data is preserved in chunks for FAQ systems.
        
        Args:
            text: Document text
            language: Language code
            
        Returns:
            List of Chunk objects containing ALL original data
        """
        # Detect document type
        doc_type = self.detect_document_type(text, language)
        logger.info(f"doc_type: {doc_type}")
        chunks = []
        
        
        # Identify document sections
        sections = self.identify_document_sections(text, language)
        
        # logger.info(f"Sections:  {sections}")  
           
        # Process each section
        for section_idx, (section_title, section_content) in enumerate(sections):
            if not section_content.strip():
                continue
            
   
            # Add section-level chunk to preserve section integrity
            section_chunk = Chunk(
                text=f"Section: {section_title}\n\n{section_content}",
                section=section_title,
                type="section",
                metadata=ChunkMetadata(
                    hasQuestion=False,
                    documentType=doc_type,
                    language=language,
                    chunkIndex=len(chunks),
                    totalChunks=0,  # Will update later
                    sectionIndex=section_idx
                )
            )
            chunks.append(section_chunk)
            
            # Determine appropriate processing for this section
            if doc_type == 'faq' or any(term in section_title.lower() for term in self.get_faq_terms(language)):
                # Process as FAQ section
                qa_pairs = self.extract_qa_pairs(section_content, language)
                
                if qa_pairs:
                    for i, (question, answer) in enumerate(qa_pairs):
                        # Create individual Q&A chunks for precise matching
                        qa_chunk = Chunk(
                            text=f"Q: {question}\nA: {answer}",
                            section=section_title,
                            type="qa_pair",
                            metadata=ChunkMetadata(
                                hasQuestion=True,
                                documentType='faq',
                                language=language,
                                qaFormat="explicit",
                                question=question,
                                answer=answer,
                                chunkIndex=len(chunks),
                                totalChunks=0,  # Will update later
                                sectionIndex=section_idx,
                                qaPairIndex=i
                            )
                        )
                        chunks.append(qa_chunk)
                        
                        # Also create separate question and answer chunks for better search
                        question_chunk = Chunk(
                            text=question,
                            section=section_title,
                            type="question",
                            metadata=ChunkMetadata(
                                hasQuestion=True,
                                documentType='faq',
                                language=language,
                                qaFormat="question_only",
                                question=question,
                                relatedAnswer=answer,
                                chunkIndex=len(chunks),
                                totalChunks=0,
                                sectionIndex=section_idx,
                                qaPairIndex=i
                            )
                        )
                        chunks.append(question_chunk)
                        
                        # Only add answer chunk if it's substantial
                        if len(answer.strip()) > 10:
                            answer_chunk = Chunk(
                                text=answer,
                                section=section_title,
                                type="answer",
                                metadata=ChunkMetadata(
                                    hasQuestion=False,
                                    documentType='faq',
                                    language=language,
                                    qaFormat="answer_only",
                                    relatedQuestion=question,
                                    chunkIndex=len(chunks),
                                    totalChunks=0,
                                    sectionIndex=section_idx,
                                    qaPairIndex=i
                                )
                            )
                            chunks.append(answer_chunk)
                
                # Always process the remaining content as well to catch any non-Q&A text
                remaining_content = self._extract_non_qa_content(section_content, qa_pairs, language)
                if remaining_content.strip():
                    content_chunks = self.chunk_text_preserving_data(remaining_content, language, section_title, section_idx)
                    chunks.extend(content_chunks)
                    
            elif doc_type == 'policy' or any(term in section_title.lower() for term in self.get_policy_terms(language)):
                # Process as policy section
                policy_chunks = self._process_policy_section(section_content, section_title, language, section_idx)
                chunks.extend(policy_chunks)
                
            else:
                # Process as general content
                content_chunks = self.chunk_text_preserving_data(section_content, language, section_title, section_idx)
                chunks.extend(content_chunks)
        
        # If no sections were found, process the entire text as content
        if len(sections) <= 1 and sections[0][0] == "General":
            content_chunks = self.chunk_text_preserving_data(text, language, "General", 0)
            chunks.extend(content_chunks)
        
        # Update total chunks count
        total_chunks = len(chunks)
        for chunk in chunks:
            chunk.metadata.totalChunks = total_chunks
            
        return chunks

    def _extract_non_qa_content(self, text: str, qa_pairs: List[Tuple[str, str]], language: str) -> str:
        """
        Extract content that is not part of Q&A pairs to ensure nothing is lost.
        
        Args:
            text: Original section text
            qa_pairs: Extracted Q&A pairs
            language: Language code
            
        Returns:
            Content that wasn't captured in Q&A pairs
        """
        remaining_text = text
        
        # Remove Q&A content from the text
        for question, answer in qa_pairs:
            # Try to find and remove the Q&A pattern
            qa_markers = self.get_qa_markers(language)
            
            for q_marker, a_marker in qa_markers:
                for separator in [':', '.', ' ']:
                    q_pattern = f"{q_marker}{separator}"
                    a_pattern = f"{a_marker}{separator}"
                    
                    # Pattern to match the full Q&A
                    full_qa_pattern = rf'\s*{re.escape(q_pattern)}\s*{re.escape(question)}\s*{re.escape(a_pattern)}\s*{re.escape(answer)}'
                    remaining_text = re.sub(full_qa_pattern, '', remaining_text, flags=re.IGNORECASE | re.DOTALL)
            
            # Also try to remove just the question and answer separately
            remaining_text = remaining_text.replace(question, '')
            remaining_text = remaining_text.replace(answer, '')
        
        # Clean up extra whitespace
        remaining_text = re.sub(r'\n\s*\n\s*\n', '\n\n', remaining_text)
        return remaining_text.strip()

    def _process_policy_section(self, section_content: str, section_title: str, language: str, section_idx: int) -> List[Chunk]:
        """
        Process policy sections ensuring all clauses and content are preserved.
        
        Args:
            section_content: Content of the policy section
            section_title: Title of the section
            language: Language code
            section_idx: Index of the section
            
        Returns:
            List of policy chunks preserving all data
        """
        chunks = []
        
        # Look for numbered clauses
        script_group = self.get_script_group(language)
        clauses = self._extract_policy_clauses(section_content, script_group)
        
        if clauses:
            # Process clauses in groups while preserving all content
            current_clauses = []
            current_size = 0
            max_size = 400  # Max words per chunk for policies (longer than regular content)
            
            for clause_idx, clause in enumerate(clauses):
                clause_size = len(clause) if script_group == 'cjk' else len(clause.split())
                
                # Create individual clause chunks for precise search
                clause_chunk = Chunk(
                    text=clause,
                    section=section_title,
                    type="policy_clause",
                    metadata=ChunkMetadata(
                        hasQuestion=False,
                        documentType='policy',
                        language=language,
                        chunkIndex=len(chunks),
                        totalChunks=0,
                        sectionIndex=section_idx,
                        clauseIndex=clause_idx
                    )
                )
                chunks.append(clause_chunk)
                
                # Group clauses for context-aware chunks
                if current_size + clause_size > max_size and current_clauses:
                    # Save current grouped chunk
                    chunk_text = "\n\n".join(current_clauses)
                    grouped_chunk = Chunk(
                        text=chunk_text,
                        section=section_title,
                        type="policy_clauses_group",
                        metadata=ChunkMetadata(
                            hasQuestion=False,
                            documentType='policy',
                            language=language,
                            chunkIndex=len(chunks),
                            totalChunks=0,
                            sectionIndex=section_idx
                        )
                    )
                    chunks.append(grouped_chunk)
                    current_clauses = [clause]
                    current_size = clause_size
                else:
                    current_clauses.append(clause)
                    current_size += clause_size
            
            # Add remaining grouped clauses
            if current_clauses:
                chunk_text = "\n\n".join(current_clauses)
                grouped_chunk = Chunk(
                    text=chunk_text,
                    section=section_title,
                    type="policy_clauses_group",
                    metadata=ChunkMetadata(
                        hasQuestion=False,
                        documentType='policy',
                        language=language,
                        chunkIndex=len(chunks),
                        totalChunks=0,
                        sectionIndex=section_idx
                    )
                )
                chunks.append(grouped_chunk)
            
            # Extract any content that wasn't captured in clauses
            remaining_content = self._extract_non_clause_content(section_content, clauses)
            if remaining_content.strip():
                remaining_chunks = self.chunk_text_preserving_data(remaining_content, language, section_title, section_idx)
                chunks.extend(remaining_chunks)
        else:
            # No specific clauses found, process as regular content
            content_chunks = self.chunk_text_preserving_data(section_content, language, section_title, section_idx)
            chunks.extend(content_chunks)
        
        return chunks

    def _extract_policy_clauses(self, text: str, script_group: str) -> List[str]:
        """Extract numbered policy clauses based on script group."""
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
            if clause:
                clauses.append(clause)
        
        return clauses

    def _extract_non_clause_content(self, text: str, clauses: List[str]) -> str:
        """Extract content that wasn't captured in policy clauses."""
        remaining_text = text
        
        # Remove clause content
        for clause in clauses:
            remaining_text = remaining_text.replace(clause, '')
        
        # Remove clause numbering patterns
        patterns = [
            r'(?:^|\n)\s*\d+\.\s*',
            r'(?:^|\n)\s*[一二三四五六七八九十]+[\.．、]\s*',
            r'(?:^|\n)\s*[१२३४५६७८९०]+[\.।]\s*',
            r'(?:^|\n)\s*[١٢٣٤٥٦٧٨٩٠]+[\.،]\s*'
        ]
        
        for pattern in patterns:
            remaining_text = re.sub(pattern, '\n', remaining_text, flags=re.MULTILINE)
        
        # Clean up extra whitespace
        remaining_text = re.sub(r'\n\s*\n\s*\n', '\n\n', remaining_text)
        return remaining_text.strip()

    def chunk_text_preserving_data(self, text: str, language: str, section_title: str, section_idx: int, max_chunk_size: int = 300, overlap_percentage: float = 0.15) -> List[Chunk]:
        """
        Split text into chunks while ensuring ALL data is preserved through overlapping and complete coverage.
        
        Args:
            text: Text to chunk
            language: Language code
            section_title: Title of the section this text belongs to
            section_idx: Index of the section
            max_chunk_size: Maximum words per chunk
            overlap_percentage: Percentage of chunk to overlap with next chunk
            
        Returns:
            List of Chunk objects ensuring no data loss
        """
        if not text.strip():
            return []
        
        # Split text into paragraphs first
        paragraphs = re.split(r'\n\s*\n', text)
        paragraphs = [p.strip() for p in paragraphs if p.strip()]
        
        if not paragraphs:
            return []
        
        # For CJK languages, count characters instead of words
        script_group = self.get_script_group(language)
        count_chars = script_group == 'cjk' or script_group == 'thai'
        
        chunks = []
        current_chunk_paragraphs = []
        current_size = 0
        
        for para_idx, paragraph in enumerate(paragraphs):
            # Determine paragraph size
            paragraph_size = len(paragraph) if count_chars else len(paragraph.split())
            
            # If this single paragraph is larger than max size, split it into sentences
            if paragraph_size > max_chunk_size:
                # Save current chunk if we have one
                if current_chunk_paragraphs:
                    chunk_text = "\n\n".join(current_chunk_paragraphs)
                    chunk = Chunk(
                        text=chunk_text,
                        section=section_title,
                        type="content",
                        metadata=ChunkMetadata(
                            hasQuestion=False,
                            documentType='general',
                            language=language,
                            chunkIndex=len(chunks),
                            totalChunks=0,
                            sectionIndex=section_idx
                        )
                    )
                    chunks.append(chunk)
                    current_chunk_paragraphs = []
                    current_size = 0
                
                # Split large paragraph into sentence-based chunks
                sentence_chunks = self._create_sentence_chunks(paragraph, language, section_title, section_idx, max_chunk_size, overlap_percentage)
                chunks.extend(sentence_chunks)
                
            # If adding this paragraph would exceed chunk size, finalize current chunk
            elif current_size > 0 and current_size + paragraph_size > max_chunk_size:
                chunk_text = "\n\n".join(current_chunk_paragraphs)
                chunk = Chunk(
                    text=chunk_text,
                    section=section_title,
                    type="content",
                    metadata=ChunkMetadata(
                        hasQuestion=False,
                        documentType='general',
                        language=language,
                        chunkIndex=len(chunks),
                        totalChunks=0,
                        sectionIndex=section_idx
                    )
                )
                chunks.append(chunk)
                current_chunk_paragraphs = [paragraph]
                current_size = paragraph_size
            else:
                # Add paragraph to current chunk
                current_chunk_paragraphs.append(paragraph)
                current_size += paragraph_size
        
        # Add any remaining paragraphs
        if current_chunk_paragraphs:
            chunk_text = "\n\n".join(current_chunk_paragraphs)
            chunk = Chunk(
                text=chunk_text,
                section=section_title,
                type="content",
                metadata=ChunkMetadata(
                    hasQuestion=False,
                    documentType='general',
                    language=language,
                    chunkIndex=len(chunks),
                    totalChunks=0,
                    sectionIndex=section_idx
                )
            )
            chunks.append(chunk)
        
        # Apply overlap between chunks to ensure context preservation
        if len(chunks) > 1 and overlap_percentage > 0:
            chunks = self._apply_chunk_overlap_to_chunks(chunks, overlap_percentage, count_chars)
        
        return chunks

    def _create_sentence_chunks(self, paragraph: str, language: str, section_title: str, section_idx: int, max_chunk_size: int, overlap_percentage: float) -> List[Chunk]:
        """Create chunks from a large paragraph by splitting into sentences."""
        sentences = self.split_into_sentences(paragraph, language)
        
        script_group = self.get_script_group(language)
        count_chars = script_group == 'cjk' or script_group == 'thai'
        
        chunks = []
        current_sentences = []
        current_size = 0
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            
            sentence_size = len(sentence) if count_chars else len(sentence.split())
            
            # If single sentence is too large, include it anyway to preserve data
            if sentence_size > max_chunk_size:
                # Save current sentences if any
                if current_sentences:
                    chunk_text = " ".join(current_sentences)
                    chunk = Chunk(
                        text=chunk_text,
                        section=section_title,
                        type="content",
                        metadata=ChunkMetadata(
                            hasQuestion=False,
                            documentType='general',
                            language=language,
                            chunkIndex=len(chunks),
                            totalChunks=0,
                            sectionIndex=section_idx
                        )
                    )
                    chunks.append(chunk)
                    current_sentences = []
                    current_size = 0
                
                # Add large sentence as its own chunk
                chunk = Chunk(
                    text=sentence,
                    section=section_title,
                    type="content",
                    metadata=ChunkMetadata(
                        hasQuestion=False,
                        documentType='general',
                        language=language,
                        chunkIndex=len(chunks),
                        totalChunks=0,
                        sectionIndex=section_idx
                    )
                )
                chunks.append(chunk)
                
            # If adding this sentence would exceed chunk size, finalize current chunk
            elif current_size > 0 and current_size + sentence_size > max_chunk_size:
                chunk_text = " ".join(current_sentences)
                chunk = Chunk(
                    text=chunk_text,
                    section=section_title,
                    type="content",
                    metadata=ChunkMetadata(
                        hasQuestion=False,
                        documentType='general',
                        language=language,
                        chunkIndex=len(chunks),
                        totalChunks=0,
                        sectionIndex=section_idx
                    )
                )
                chunks.append(chunk)
                current_sentences = [sentence]
                current_size = sentence_size
            else:
                current_sentences.append(sentence)
                current_size += sentence_size
        
        # Add remaining sentences
        if current_sentences:
            chunk_text = " ".join(current_sentences)
            chunk = Chunk(
                text=chunk_text,
                section=section_title,
                type="content",
                metadata=ChunkMetadata(
                    hasQuestion=False,
                    documentType='general',
                    language=language,
                    chunkIndex=len(chunks),
                    totalChunks=0,
                    sectionIndex=section_idx
                )
            )
            chunks.append(chunk)
        
        return chunks

    def _apply_chunk_overlap_to_chunks(self, chunks: List[Chunk], overlap_percentage: float, count_chars: bool) -> List[Chunk]:
        """Apply overlap between consecutive chunks to preserve context."""
        if len(chunks) <= 1:
            return chunks
        
        overlapped_chunks = []
        
        for i, chunk in enumerate(chunks):
            current_text = chunk.text
            
            # Add overlap from previous chunk (except for first chunk)
            if i > 0:
                prev_chunk = chunks[i - 1]
                overlap_size = int(len(prev_chunk.text) * overlap_percentage) if count_chars else int(len(prev_chunk.text.split()) * overlap_percentage)
                
                if count_chars:
                    # For character-based languages, take last N characters
                    prev_overlap = prev_chunk.text[-overlap_size:] if overlap_size > 0 else ""
                else:
                    # For word-based languages, take last N words
                    prev_words = prev_chunk.text.split()
                    prev_overlap = " ".join(prev_words[-overlap_size:]) if overlap_size > 0 else ""
                
                if prev_overlap.strip():
                    current_text = prev_overlap + " " + current_text
            
            # Create new chunk with overlapped content
            overlapped_chunk = Chunk(
                text=current_text,
                section=chunk.section,
                type=chunk.type + "_overlapped",
                metadata=ChunkMetadata(
                    hasQuestion=chunk.metadata.hasQuestion,
                    documentType=chunk.metadata.documentType,
                    language=chunk.metadata.language,
                    chunkIndex=chunk.metadata.chunkIndex,
                    totalChunks=chunk.metadata.totalChunks,
                    sectionIndex=chunk.metadata.sectionIndex,
                    hasOverlap=True
                )
            )
            overlapped_chunks.append(overlapped_chunk)
        
        return overlapped_chunks

    def chunk_text(self, text: str, language: str, max_chunk_size: int = 300, overlap_percentage: float = 0.15) -> List[str]:
        """
        Split text into appropriate sized chunks with language awareness and overlap for context preservation.
        
        Args:
            text: Text to chunk
            language: Language code
            max_chunk_size: Maximum words per chunk
            overlap_percentage: Percentage of chunk to overlap with next chunk (0.1 to 0.2 recommended)
            
        Returns:
            List of text chunks with overlap
        """
        # Split text into paragraphs
        paragraphs = re.split(r'\n\s*\n', text)
        
        # For CJK languages, which don't use spaces to separate words,
        # we'll count characters instead of words
        script_group = self.get_script_group(language)
        count_chars = script_group == 'cjk' or script_group == 'thai'
        
        chunks = []
        current_chunk = []
        current_size = 0
        
        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if not paragraph:
                continue
            
            # Skip very short paragraphs that are likely headers
            words = paragraph.split() if not count_chars else [paragraph]
            if not count_chars and len(words) < 3:
                # If it looks like a header, add it to the current chunk
                if current_chunk and re.match(r'^(?:\d+\.|\*|\-|\#)\s*[A-Z]', paragraph):
                    current_chunk.append(paragraph)
                continue
            
            # Determine paragraph size (words or characters)
            paragraph_size = len(paragraph) if count_chars else len(words)
            
            # If this paragraph would exceed our chunk size limit, split it into sentences
            if paragraph_size > max_chunk_size:
                # Add current chunk if we have one
                if current_chunk:
                    chunks.append("\n\n".join(current_chunk))
                    current_chunk = []
                    current_size = 0
                
                # Split paragraph into sentences
                sentences = self.split_into_sentences(paragraph, language)
                
                # Process sentences
                current_sentences = []
                current_sentences_size = 0
                
                for sentence in sentences:
                    sentence = sentence.strip()
                    if not sentence:
                        continue
                    
                    # Determine sentence size
                    sentence_size = len(sentence) if count_chars else len(sentence.split())
                    
                    # If this sentence would exceed our chunk size, add it as its own chunk
                    if sentence_size > max_chunk_size:
                        # Add current sentences if we have any
                        if current_sentences:
                            chunks.append(" ".join(current_sentences))
                            current_sentences = []
                            current_sentences_size = 0
                        
                        # Add long sentence as its own chunk
                        chunks.append(sentence)
                    
                    # If adding this sentence would exceed our chunk size, finalize the current chunk
                    elif current_sentences_size > 0 and current_sentences_size + sentence_size > max_chunk_size:
                        chunks.append(" ".join(current_sentences))
                        current_sentences = [sentence]
                        current_sentences_size = sentence_size
                    else:
                        # Add to current sentences
                        current_sentences.append(sentence)
                        current_sentences_size += sentence_size
                
                # Add any remaining sentences
                if current_sentences:
                    chunks.append(" ".join(current_sentences))
            
            # If adding this paragraph would exceed our chunk size, start a new chunk
            elif current_size > 0 and current_size + paragraph_size > max_chunk_size:
                chunks.append("\n\n".join(current_chunk))
                current_chunk = [paragraph]
                current_size = paragraph_size
            else:
                # Add to current chunk
                current_chunk.append(paragraph)
                current_size += paragraph_size
        
        # Add any remaining content
        if current_chunk:
            chunks.append("\n\n".join(current_chunk))
        
        # Apply overlap to chunks for context preservation
        if len(chunks) > 1 and overlap_percentage > 0:
            chunks = self._apply_chunk_overlap(chunks, overlap_percentage, count_chars)
        
        # Ensure we don't have empty chunks
        return [chunk for chunk in chunks if chunk.strip()]

    def _apply_chunk_overlap(self, chunks: List[str], overlap_percentage: float, count_chars: bool) -> List[str]:
        """Apply overlap between consecutive chunks to preserve context"""
        if len(chunks) <= 1:
            return chunks
        
        overlapped_chunks = []
        
        for i, chunk in enumerate(chunks):
            current_chunk = chunk
            
            # Add overlap from previous chunk (except for first chunk)
            if i > 0:
                prev_chunk = chunks[i - 1]
                overlap_size = int(len(prev_chunk) * overlap_percentage) if count_chars else int(len(prev_chunk.split()) * overlap_percentage)
                
                if count_chars:
                    # For character-based languages, take last N characters
                    prev_overlap = prev_chunk[-overlap_size:] if overlap_size > 0 else ""
                else:
                    # For word-based languages, take last N words
                    prev_words = prev_chunk.split()
                    prev_overlap = " ".join(prev_words[-overlap_size:]) if overlap_size > 0 else ""
                
                if prev_overlap.strip():
                    current_chunk = prev_overlap + " " + current_chunk
            
            overlapped_chunks.append(current_chunk)
        
        return overlapped_chunks


    async def _setup_elasticsearch_indices(self):
        """Setup Elasticsearch indices with proper mappings for vector search and simplified dynamic keywords"""

        # Chunks index with vector field and essential keyword fields
        chunks_index = self.es_config['tenant_document_index_name']
        exists = await self.es_client.indices.exists(index=chunks_index)
        if not exists:
            await self.es_client.indices.create(
                index=chunks_index,
                settings={
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                    "analysis": {
                        "analyzer": {
                            "keyword_analyzer": {
                                "type": "custom",
                                "tokenizer": "keyword",
                                "filter": ["lowercase", "trim"]
                            }
                        }
                    }
                },
                mappings={
                    "properties": {
                        # Core content fields
                        "content": {"type": "text"},
                        "contentVector": {
                            "type": "dense_vector",
                            "dims": 768,  # mpnet-base-v2 has 768 dimensions
                            "index": True,
                            "similarity": "cosine"
                        },
                        
                        # Identification fields
                        "tenantId": {"type": "keyword"},
                        "documentId": {"type": "keyword"},
                        "chunkType": {"type": "keyword"},
                        "sectionTitle": {"type": "text"},
                        "metadata": {"type": "object", "enabled": True},
                        
                        # Structure and context
                        "chunkPosition": {"type": "integer"},
                        "totalChunks": {"type": "integer"},
                        "hasOverlap": {"type": "boolean"},
                        "contextSummary": {"type": "text"},
                        
                        # Simplified dynamic keywords (extracted by Mistral)
                        "keywords": {
                            "type": "keyword",
                            "ignore_above": 100
                        },
                        "keywordsText": {
                            "type": "text",
                            "analyzer": "keyword_analyzer"
                        },
                        
                        # Essential metadata
                        "language": {"type": "keyword"},
                        "keywordCount": {"type": "integer"}
                    }
                }
            )
            logger.info(f"Created simplified chunks index: {chunks_index}")
        else:
            # Check if index needs updating for new fields
            try:
                current_mapping = await self.es_client.indices.get_mapping(index=chunks_index)
                current_properties = current_mapping[chunks_index]['mappings'].get('properties', {})
                
                # Essential new fields only
                new_fields = {
                    "keywords": {
                        "type": "keyword",
                        "ignore_above": 100
                    },
                    "keywordsText": {
                        "type": "text",
                        "analyzer": "keyword_analyzer"
                    },
                    "language": {"type": "keyword"},
                    "keywordCount": {"type": "integer"}
                }
                
                # Add missing fields if any
                fields_to_add = {}
                for field_name, field_mapping in new_fields.items():
                    if field_name not in current_properties:
                        fields_to_add[field_name] = field_mapping
                
                if fields_to_add:
                    logger.info(f"Adding {len(fields_to_add)} new fields to existing index")
                    await self.es_client.indices.put_mapping(
                        index=chunks_index,
                        properties=fields_to_add
                    )
                    logger.info(f"Successfully added new fields: {list(fields_to_add.keys())}")
                    
            except Exception as e:
                logger.warning(f"Could not update index mapping: {str(e)}")


    async def extract_keywords_and_context_with_mistral(self, text: str, section_title: str = "", language: str = 'en', max_keywords: int = 20) -> Dict[str, Any]:
        """
        Extract keywords and context summary dynamically using Mistral for any language and domain
        
        Args:
            text: The chunk text to extract keywords and context from
            section_title: The section title for additional context
            language: Language code for the text
            max_keywords: Maximum number of keywords to extract
            
        Returns:
            Dictionary containing keywords and context summary only
        """
        try:
            # Simplified system prompt for only keywords and context_summary
            system_prompt = f"""Extract {max_keywords} keywords and create a context summary from the text. Return ONLY JSON:
    {{
        "keywords": ["word1", "phrase1", "concept1"],
        "context_summary": "Brief 1-2 sentence summary of the main content and purpose"
    }}

    Rules for keywords:
    - Same language as input text
    - Important nouns, technical terms, key phrases
    - No stop words or articles
    - Max 3 words per phrase
    - Focus on searchable terms

    Rules for context_summary:
    - 1-2 sentences maximum
    - Capture main purpose/content
    - Same language as input
    - Clear and concise"""

            # Enhanced user prompt with section context
            section_context = f"Section: {section_title}\n\n" if section_title and section_title != "General" else ""
            user_prompt = f"{section_context}Text: {text[:1200]}"

            # Prepare payload for Mistral
            data = {
                "model": MISTRAL_CONFIG['model'],
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "stream": False,
                "max_tokens": 250,
                "temperature": 0.1
            }

            # logger.info(f"Extracting keywords and context for chunk in language: {language}, data: {data}")
            
            response = await self.http_client.post(
                MISTRAL_CONFIG['chat_url'],
                headers={"Content-Type": "application/json"},
                json=data,
                timeout=MISTRAL_CONFIG['timeout']
            )
            
            logger.info(f"Mistral API response status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"Mistral extraction error: {response.status_code} - {response.text}")
                return self._fallback_extraction(text, section_title, language, max_keywords)

            response_data = response.json()
            
            # logger.info(f"Mistral response received for keyword and context extraction, response_data: {response_data}")
            # Extract durations (they are in nanoseconds)
            total_duration_ns = response_data.get("total_duration", 0)
            load_duration_ns = response_data.get("load_duration", 0)
            prompt_eval_duration_ns = response_data.get("prompt_eval_duration", 0)
            eval_duration_ns = response_data.get("eval_duration", 0)

            # Convert to seconds
            total_duration_sec = total_duration_ns / 1e9
            load_duration_sec = load_duration_ns / 1e9
            prompt_eval_duration_sec = prompt_eval_duration_ns / 1e9
            eval_duration_sec = eval_duration_ns / 1e9

            # Print in seconds
            logger.info(
                f"Mistral durations (in seconds): "
                f"total={total_duration_sec:.3f}s, "
                f"load={load_duration_sec:.3f}s, "
                f"prompt_eval={prompt_eval_duration_sec:.3f}s, "
                f"eval={eval_duration_sec:.3f}s"
            )
            if 'message' in response_data and 'content' in response_data['message']:
                content = response_data['message']['content'].strip()
                # logger.info(f"Extracted content from Mistral: {content}")
                
                try:
                    # Parse JSON response
                    extraction_data = json.loads(content)
                    # logger.info(f"Successfully parsed JSON: {extraction_data}")
                    
                    # Validate and process the response
                    processed_data = self._process_mistral_extraction(extraction_data, text, section_title, max_keywords)
                    
                    # logger.info(f"Successfully extracted {len(processed_data.get('keywords', []))} keywords and context")
                    return processed_data
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse Mistral JSON response: {e}")
                    logger.error(f"Raw response: {content}")
                    return self._extract_from_text_response(content, text, section_title, max_keywords)
            
            logger.error("No message in response or missing content")
            return self._fallback_extraction(text, section_title, language, max_keywords)
            
        except Exception as e:
            logger.error(f"Error in Mistral keyword and context extraction: {type(e).__name__}: {str(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return self._fallback_extraction(text, section_title, language, max_keywords)
    
    def _process_mistral_extraction(self, extraction_data: Dict, text: str, section_title: str, max_keywords: int) -> Dict[str, Any]:
        """
        Process and validate Mistral extraction response - simplified to only handle keywords and context_summary
        
        Args:
            extraction_data: Raw data from Mistral
            text: Original text for fallback
            section_title: Section title
            max_keywords: Maximum keywords
            
        Returns:
            Processed extraction data with only keywords and context_summary
        """
        processed = {
            "keywords": [],
            "context_summary": ""
        }
        
        # Process keywords
        if 'keywords' in extraction_data and isinstance(extraction_data['keywords'], list):
            # Clean and validate keywords
            keywords = []
            for keyword in extraction_data['keywords'][:max_keywords]:
                if isinstance(keyword, str) and len(keyword.strip()) > 1:
                    cleaned = keyword.strip().lower()
                    # Remove common stop words and very short words
                    if len(cleaned) > 2 and cleaned not in ['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by']:
                        keywords.append(keyword.strip())
            processed["keywords"] = keywords
        else:
            # Fallback keywords extraction
            processed["keywords"] = self._extract_fallback_keywords(text, max_keywords)
        
        # Process context summary
        if 'context_summary' in extraction_data and isinstance(extraction_data['context_summary'], str):
            summary = extraction_data['context_summary'].strip()
            if len(summary) > 10:  # Ensure it's not too short
                processed["context_summary"] = summary
            else:
                processed["context_summary"] = self._create_fallback_summary(text, section_title)
        else:
            processed["context_summary"] = self._create_fallback_summary(text, section_title)
        
        return processed

    def _extract_fallback_keywords(self, text: str, max_keywords: int) -> List[str]:
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

    def _fallback_extraction(self, text: str, section_title: str, language: str, max_keywords: int) -> Dict[str, Any]:
        """
        Fallback extraction when Mistral fails - simplified to only return keywords and context_summary
        
        Args:
            text: Text to process
            section_title: Section title
            language: Language code
            max_keywords: Maximum keywords
            
        Returns:
            Basic extraction data with only keywords and context_summary
        """
        logger.info("Using fallback extraction method")
        
        # Extract keywords using simple method
        keywords = self._extract_fallback_keywords(text, max_keywords)
        
        # Create basic summary
        summary = self._create_fallback_summary(text, section_title)
        
        return {
            "keywords": keywords,
            "context_summary": summary
        }
        
    def _create_fallback_summary(self, text: str, section_title: str) -> str:
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

    def _extract_from_text_response(self, content: str, text: str, section_title: str, max_keywords: int) -> Dict[str, Any]:
        """
        Extract keywords and summary from non-JSON text response
        
        Args:
            content: Raw text response from Mistral
            text: Original text for fallback
            section_title: Section title
            max_keywords: Maximum keywords
            
        Returns:
            Extracted data with keywords and context_summary
        """
        logger.info("Attempting to extract from text response")
        
        # Try to find keywords in the response
        keywords = []
        lines = content.split('\n')
        
        for line in lines:
            line = line.strip()
            # Look for keyword-like patterns
            if any(keyword_indicator in line.lower() for keyword_indicator in ['keyword', 'key', 'term', 'concept']):
                # Extract potential keywords from this line
                words = line.split()
                for word in words:
                    cleaned = word.strip('.,;:!?()[]{}')
                    if (len(cleaned) > 2 and 
                        cleaned.lower() not in ['keywords', 'key', 'terms', 'concepts', 'the', 'and', 'or'] and
                        len(keywords) < max_keywords):
                        keywords.append(cleaned)
        
        # If no keywords found, use fallback
        if not keywords:
            keywords = self._extract_fallback_keywords(text, max_keywords)
        
        # Try to find summary in the response
        summary = ""
        for line in lines:
            if len(line.strip()) > 20:  # Look for longer lines that might be summaries
                summary = line.strip()
                break
        
        # If no summary found, create fallback
        if not summary:
            summary = self._create_fallback_summary(text, section_title)
        
        return {
            "keywords": keywords[:max_keywords],
            "context_summary": summary
        }

    def _parse_keywords_from_text(self, text: str) -> List[str]:
        """Parse keywords from text line"""
        keywords = []
        
        # Remove common formatting
        text = text.replace('[', '').replace(']', '').replace('"', '').replace("'", '')
        
        # Split by common separators
        for separator in [',', ';', '|', '\n']:
            if separator in text:
                parts = text.split(separator)
                for part in parts:
                    keyword = part.strip()
                    if len(keyword) > 2 and keyword.lower() not in ['and', 'or', 'the', 'a', 'an']:
                        keywords.append(keyword)
                break
        else:
            # No separator found, try to extract words
            words = text.split()
            for word in words:
                word = word.strip('.,;:!?')
                if len(word) > 2:
                    keywords.append(word)
        
        return keywords

        
    async def process_document_message_simplified(self, message: Dict[str, Any]):
        """
        Simplified document processing with essential keyword extraction
        """
        # Extract text content from message
        content = message.get('content')
        if not content:
            logger.warning("Message has no content")
            return

        tenant_id = message.get('tenantId')
        metadata = message.get('metadata', {}) or {}
        document_id = message.get('documentId')
        
        # Check if language is specified in metadata, otherwise detect it
        language = metadata.get('language')
        if not language:
            language = self.detect_best_language(content)
            metadata['language'] = language
    
        # Extract semantic chunks with language awareness
        chunks = self.extract_chunks(content, language)
        logger.info(f"Extracted {len(chunks)} semantic chunks from document")
        # logger.info(f"chunks data: {(chunks)} ")
        # Process and index each chunk
        indexing_tasks = []
        for i, chunk in enumerate(chunks):
            # Generate vector embedding for the chunk
            vector = self.st_model.encode(chunk.text).tolist()
            
            # Extract keywords using simplified Mistral approach
            extracted_keywords = await self.extract_keywords_and_context_with_mistral(
                chunk.text, chunk.section,
                language, 
                max_keywords=15
            )
            
            # Get the main keywords list (simplified structure)
            if 'keywords' in extracted_keywords:
                main_keywords = extracted_keywords['keywords']
            else:
                # Fallback: combine all categories
                main_keywords = []
                for keyword_list in extracted_keywords.values():
                    if isinstance(keyword_list, list):
                        main_keywords.extend(keyword_list)
            
            # Remove duplicates
            unique_keywords = list(dict.fromkeys(main_keywords))
            
            # Create searchable keywords text
            keywords_text = ' '.join(unique_keywords)
            
            if 'context_summary' in extracted_keywords:
                context_summary = extracted_keywords['context_summary']
            else:
                # Create context summary
                context_summary = self._create_context_summary(chunk.text, chunk.section, unique_keywords[:3])
            
            # Add metadata
            combined_metadata = {**metadata}
            combined_metadata.update(chunk.metadata.model_dump())
            combined_metadata['chunkIndex'] = i
            combined_metadata['totalChunks'] = len(chunks)
            combined_metadata['hasOverlap'] = i > 0
            combined_metadata['language'] = language
            
            # Simplified chunk document
            chunk_doc = {
                'content': chunk.text,
                'contentVector': vector,
                'tenantId': tenant_id,
                'documentId': document_id,
                'chunkType': chunk.type,
                'sectionTitle': chunk.section,
                'metadata': combined_metadata,
                
                # Structure
                'chunkPosition': i,
                'totalChunks': len(chunks),
                'hasOverlap': i > 0,
                'contextSummary': context_summary,
                
                # Simplified keywords
                'keywords': unique_keywords,
                'keywordsText': keywords_text,
                'language': language,
                'keywordCount': len(unique_keywords)
            }
            
            # Index the chunk
            chunk_id = f"{document_id}_chunk_{i}"
            indexing_tasks.append(
                self.es_client.index(
                    index=self.es_config['tenant_document_index_name'],
                    document=chunk_doc,
                    id=chunk_id
                )
            )
        
        # Wait for all indexing tasks to complete
        await asyncio.gather(*indexing_tasks)
        

        await self.publish_kafka_message(
            self.response_topic,
            tenant_id,
            DataStorageDto(
                tenantId=tenant_id,
                storedIds=[document_id],
                isDone=True,
                dataType ='doc'
            )
        )

        # Continue with existing cleanup and response logic...
        logger.info(f"Successfully indexed {len(chunks)} chunks with simplified keywords for document {document_id}")

    def _create_context_summary(self, chunk_text: str, section: str, top_keywords: List[str]) -> str:
        """Create a simple context summary"""
        first_sentence = chunk_text.split('.')[0][:150]
        keywords_str = ', '.join(top_keywords) if top_keywords else ''
        
        if keywords_str:
            return f"From {section}: {first_sentence}... [Keywords: {keywords_str}]"
        else:
            return f"From {section}: {first_sentence}..."


    async def process_faq_message(self, message: Dict[str, Any]):
        """
        Process an individual message and store it in Elasticsearch with vector embeddings.
        
        Args:
            message: Message dictionary containing content array and metadata.
        """
        # Extract message data
        tenant_id = message.get('tenantId')
        metadata = message.get('metadata', {}) or {}
        content_records = message.get('content', []) or []
        
        # Track all document IDs for response
        processed_faq_ids = set()
        
        # Process each content record in the array
        for content in content_records:
            faq_id = content.get('faqId')
            if not faq_id:
                logger.warning(f"Skipping record with missing faqId for tenant: {tenant_id}")
                continue
                
            processed_faq_ids.add(faq_id)
            record_metadata = metadata.copy()
            record_metadata["faqId"] = faq_id
            
            question = content.get("question", "")
            answer = content.get("answer", "")
            link = content.get("link", "")
            paraphrases = content.get('paraphrases', []) or []

            # Check if language is specified in metadata, otherwise detect it
            language = record_metadata.get('language')
            if not language:
                language = self.detect_best_language(question + " " + answer)
                record_metadata['language'] = language
            
            # Process main question and answer
            document_id = str(uuid.uuid4())
            store = f"Question: {question} \n Answer: {answer} \n Link: {link}"
            
            # Generate vector embedding for the chunk
            vector = self.st_model.encode(store).tolist()
            
            # Prepare chunk document
            document = {
                'content': store,
                'contentVector': vector,
                'tenantId': tenant_id,
                'documentId': document_id,
                'chunkType': "faq",
                'sectionTitle': "faq",
                'metadata': record_metadata
            }
                
            # Index the chunk
            await self.es_client.index(
                index=self.es_config['tenant_document_index_name'],
                document=document,
                id=document_id
            )
            
            # Process each paraphrase
            for paraphrase in paraphrases:
                paraphrase_id = str(uuid.uuid4())
                store = f"Question: {paraphrase} \n Answer: {answer} \n Link: {link}"
                
                # Generate vector embedding for the paraphrase
                vector = self.st_model.encode(store).tolist()
                
                # Prepare paraphrase document
                document = {
                    'content': store,
                    'contentVector': vector,
                    'tenantId': tenant_id,
                    'documentId': paraphrase_id,
                    'chunkType': "faq",
                    'sectionTitle': "faq",
                    'metadata': record_metadata
                }
                    
                # Index the paraphrase
                await self.es_client.index(
                    index=self.es_config['tenant_document_index_name'],
                    document=document,
                    id=paraphrase_id
                )
        
        # Delete old inactive documents for this tenant
        delete_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"tenantId": tenant_id}},
                        {"term": {"metadata.inactive": True}}
                    ]
                }
            }
        }
        
        # Delete from the index
        await self.es_client.delete_by_query(
            index=self.es_config['tenant_document_index_name'], 
            body=delete_query
        )
        
        logger.info(f"Deleted old inactive documents for tenant_id: {tenant_id}")
        
        # Send success response - using the set of all processed FAQ IDs
        
        await self.publish_kafka_message(
            self.response_topic,
            tenant_id,
            DataStorageDto(
                tenantId=tenant_id,
                storedIds=list(processed_faq_ids),
                isDone=True,
                dataType ='faq'
            )
        )
            
        
        logger.info(f"Successfully indexed {len(processed_faq_ids)} FAQs for tenant: {tenant_id}")
        



    async def publish_kafka_message(self, topic: str, key: str, message: Any):
        """
        Publish a message to a Kafka topic.
        
        Args:
            topic: Kafka topic.
            key: Message key.
            message: Message payload (will be JSON serialized).
            
        Returns:
            Future for the message delivery.
        """
        serialized_key = str(key).encode("utf-8")
        
        if isinstance(message, BaseModel):
            # Custom encoder that can handle sets
            def set_encoder(obj):
                if isinstance(obj, set):
                    return list(obj)
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            serialized_value = json.dumps(message.model_dump(), default=set_encoder).encode("utf-8")
        else:
            def set_encoder(obj):
                if isinstance(obj, set):
                    return list(obj)
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            serialized_value = json.dumps(message, default=set_encoder).encode("utf-8")
        # Create an asyncio Future to wait for delivery report
        future = asyncio.Future()
        
        def delivery_callback(err, msg):
            if err:
                future.set_exception(Exception(f"Message delivery failed: {err} for key: {key}"))
            else:
                future.set_result(msg)
        
        # Get current trackingId and ensure it's a string
        tracking_id = tracking_id_var.get() or "NA"
        if isinstance(tracking_id, bytes):
            tracking_id_str = tracking_id.decode("utf-8")
        else:
            tracking_id_str = str(tracking_id)

        self.producer.produce(
            topic,
            key=serialized_key,
            value=serialized_value,
            callback=delivery_callback,
            headers=[("X-Tracking-ID", tracking_id_str)]
        )
        self.producer.poll(1)  # Trigger delivery callbacks
        self.producer.flush()
        
        return await future

    async def send_to_dead_letter_queue(self, dlq_topic: str, message: Dict[str, Any], error: str):
        """
        Send problematic messages to a dead letter topic.
        
        Args:
            message: Original message.
            error: Error description.
        """
         # Use epoch timestamp (seconds since epoch) instead of formatted datetime string
        epoch_timestamp = int(datetime.datetime.now().timestamp())
    
        error_message = {
            "originalMessage": message,
            "error": error,
            "eventTime": epoch_timestamp
        }
        
        await self.publish_kafka_message(
           dlq_topic,
            message.get('tenantId', 'unknown'),
            error_message
        )
        logger.info(f"Message sent to dead_letter_topic due to: {error}")

    def _parse_message(self, message_value: Any) -> Dict[str, Any]:
        """
        Parse message value into a dictionary.
        
        Args:
            message_value: Raw message value.
            
        Returns:
            Parsed message as a dictionary.
        """
        if isinstance(message_value, bytes):
            try:
                return json.loads(message_value.decode('utf-8'))
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse message as JSON: {e}")
                return {"raw_content": message_value.decode('utf-8', errors='replace')}
        elif isinstance(message_value, str):
            try:
                return json.loads(message_value)
            except json.JSONDecodeError:
                return {"raw_content": message_value}
        elif isinstance(message_value, dict):
            return message_value
        else:
            return {"raw_content": str(message_value)}


    async def _test_elasticsearch_connection(self) -> bool:
        """Test Elasticsearch connectivity and health"""
        logger.info("Testing Elasticsearch connection...")
        try:
            # Test basic connectivity
            info = await self.es_client.info()
            logger.info(f"✓ Elasticsearch connection successful")
            logger.info(f"  - Cluster name: {info.get('cluster_name', 'unknown')}")
            logger.info(f"  - Version: {info.get('version', {}).get('number', 'unknown')}")
            
            # Test cluster health
            health = await self.es_client.cluster.health()
            status = health.get('status', 'unknown')
            logger.info(f"  - Cluster status: {status}")
            
            if status in ['green', 'yellow']:
                logger.info("✓ Elasticsearch cluster is healthy")
                return True
            else:
                logger.warning(f"⚠ Elasticsearch cluster status is: {status}")
                return False
                
        except Exception as e:
            logger.error(f"✗ Elasticsearch connection test failed: {e}")
            return False

    async def _test_kafka_connectivity(self) -> bool:
        """Test Kafka connectivity"""
        logger.info("Testing Kafka connectivity...")
        
        # Test producer connectivity
        try:
            test_producer = Producer(self.producer_config)
            # Get metadata to test connectivity
            metadata = test_producer.list_topics(timeout=10)
            logger.info(f"✓ Kafka producer connection successful")
            logger.info(f"  - Available topics: {len(metadata.topics)} topics found")
            
            # Check if our required topics exist
            available_topics = set(metadata.topics.keys())
            required_topics = {
                self.document_request_topic,
                self.document_dlq_topic,
                self.faq_request_topic,
                self.faq_dlq_topic,
                self.response_topic
            }
            
            missing_topics = required_topics - available_topics
            if missing_topics:
                logger.warning(f"⚠ Missing required topics: {missing_topics}")
                logger.warning("Topics will be auto-created if Kafka allows it")
            else:
                logger.info("✓ All required Kafka topics are available")
            
            test_producer.flush()
            return True
            
        except Exception as e:
            logger.error(f"✗ Kafka connectivity test failed: {e}")
            return False



    async def run(self):
        """Main processing loop with comprehensive startup checks."""
        logger.info("=" * 60)
        logger.info("STARTING MULTILINGUAL MESSAGE PROCESSOR")
        logger.info("=" * 60)
        
        startup_success = True
        
        try:
            # Test Elasticsearch connectivity
            if not await self._test_elasticsearch_connection():
                startup_success = False
                logger.error("✗ STARTUP FAILED: Elasticsearch connectivity check failed")
            
            # Initialize Kafka producer
            logger.info("Initializing Kafka producer...")
            try:
                self.producer = Producer(self.producer_config)
                logger.info("✓ Kafka producer initialized successfully")
                
                # Test Kafka connectivity
                if not await self._test_kafka_connectivity():
                    startup_success = False
                    logger.error("✗ STARTUP FAILED: Kafka connectivity check failed")
                    
            except Exception as e:
                startup_success = False
                logger.error(f"✗ STARTUP FAILED: Kafka producer initialization failed: {e}")
            
            # Setup Elasticsearch indices
            try:
                await self._setup_elasticsearch_indices()
            except Exception as e:
                startup_success = False
                logger.error(f"✗ STARTUP FAILED: Elasticsearch setup failed: {e}")
            
            if not startup_success:
                logger.error("=" * 60)
                logger.error("SYSTEM STARTUP FAILED - CRITICAL ERRORS DETECTED")
                logger.error("Please check the logs above and fix connectivity issues")
                logger.error("=" * 60)
                raise Exception("System startup failed due to connectivity issues")
            
            # Log successful startup
            logger.info("=" * 60)
            logger.info("🎉 SYSTEM STARTUP SUCCESSFUL! 🎉")
            logger.info("✓ All connectivity checks passed")
            logger.info("✓ Elasticsearch: Connected and healthy")
            logger.info("✓ Kafka: Producer and topics verified")
            logger.info("✓ SentenceTransformer: Model loaded")
            logger.info("✓ Language Detection: Ready")
            logger.info("=" * 60)
            logger.info("System is now ready to process messages...")
            
            # Create separate methods for the consumer loops
            async def document_consumer_loop():
                logger.info(f"Starting document consumer for topic: {self.document_request_topic}")
                consumer = Consumer(self.consumer_config)
                
                try:
                    consumer.subscribe([self.document_request_topic])
                    logger.info(f"✓ Document consumer subscribed to topic: {self.document_request_topic}")
                    
                    while not self.shutdown_requested:
                        msg = consumer.poll(1.0)
                        if msg is None:
                            await asyncio.sleep(0.1)
                            continue
                        
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                logger.debug(f"Reached end of partition {msg.partition()}")
                            else:
                                logger.error(f"Document consumer error: {msg.error()}")
                            continue
                        
                        # --- Set the tracking ID from Kafka headers before logging ---
                        kafka_headers = dict(msg.headers() or [])
                        tracking_id = kafka_headers.get('X-Tracking-ID', b'NA')
                        tracking_id_var.set(tracking_id.decode('utf-8') if isinstance(tracking_id, bytes) else str(tracking_id))

                        # Process message
                        try:
                            value = self._parse_message(msg.value())
                            logger.info(f"📄 Processing document message (partition: {msg.partition()}, offset: {msg.offset()})")
                            await self.process_document_message_simplified(value)
                            logger.info("✓ Document message processed successfully")
                            
                        except Exception as e:
                            logger.error(f"✗ Error processing document message: {str(e)}", exc_info=True)
                            await self.send_to_dead_letter_queue(
                                self.document_dlq_topic,
                                self._parse_message(msg.value()) if msg.value() else {},
                                str(e)
                            )
                            
                except Exception as e:
                    logger.error(f"✗ CRITICAL: Document consumer error: {e}", exc_info=True)
                    raise
                finally:
                    consumer.close()
                    logger.info("📄 Document Kafka consumer closed")

            async def faq_consumer_loop():
                logger.info(f"Starting FAQ consumer for topic: {self.faq_request_topic}")
                consumer = Consumer(self.consumer_config)
                
                try:
                    consumer.subscribe([self.faq_request_topic])
                    logger.info(f"✓ FAQ consumer subscribed to topic: {self.faq_request_topic}")
                    
                    while not self.shutdown_requested:
                        msg = consumer.poll(1.0)
                        if msg is None:
                            await asyncio.sleep(0.1)
                            continue
                        
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                logger.debug(f"Reached end of partition {msg.partition()}")
                            else:
                                logger.error(f"FAQ consumer error: {msg.error()}")
                            continue
                        
                        # Process message
                        try:
                            value = self._parse_message(msg.value())
                            logger.info(f"❓ Processing FAQ message (partition: {msg.partition()}, offset: {msg.offset()})")
                            await self.process_faq_message(value)
                            logger.info("✓ FAQ message processed successfully")
                            
                        except Exception as e:
                            logger.error(f"✗ Error processing FAQ message: {str(e)}", exc_info=True)
                            await self.send_to_dead_letter_queue(
                                self.faq_dlq_topic,
                                self._parse_message(msg.value()) if msg.value() else {},
                                str(e)
                            )
                            
                except Exception as e:
                    logger.error(f"✗ CRITICAL: FAQ consumer error: {e}", exc_info=True)
                    raise
                finally:
                    consumer.close()
                    logger.info("❓ FAQ Kafka consumer closed")

            # Start consuming messages concurrently
            await asyncio.gather(
                document_consumer_loop(),
                faq_consumer_loop()
            )
            
        except KeyboardInterrupt:
            logger.info("⚠ Keyboard interrupt received, shutting down gracefully...")
        except Exception as e:
            logger.error(f"✗ CRITICAL: Fatal error in main loop: {str(e)}", exc_info=True)
            logger.error("=" * 60)
            logger.error("SYSTEM ENCOUNTERED A FATAL ERROR")
            logger.error("=" * 60)
            raise
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Graceful shutdown of resources."""
        logger.info("=" * 40)
        logger.info("INITIATING GRACEFUL SHUTDOWN")
        logger.info("=" * 40)
        
        try:
            # Close the Elasticsearch client
            logger.info("Closing Elasticsearch connection...")
            await self.es_client.close()
            logger.info("✓ Elasticsearch connection closed")
            
            # Ensure all messages are delivered before shutting down producer
            if self.producer:
                logger.info("Flushing Kafka producer...")
                self.producer.flush()
                logger.info("✓ Kafka producer flushed")
            
            logger.info("=" * 40)
            logger.info("✓ GRACEFUL SHUTDOWN COMPLETED")
            logger.info("=" * 40)
            
        except Exception as e:
            logger.error(f"✗ Error during shutdown: {e}")


if __name__ == "__main__":
    # Initialize processor
    try:
        logger.info("🚀 Starting Multilingual Message Processor Application")
        processor = MultilingualMessageProcessor(ES_CONFIG, KAFKA_CONFIG)
        
        # Run the processor
        asyncio.run(processor.run())
        
    except KeyboardInterrupt:
        logger.info("👋 Application stopped by user")
    except Exception as e:
        logger.error(f"💥 Application failed to start: {e}", exc_info=True)
        sys.exit(1)