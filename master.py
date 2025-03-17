import re
import json
import logging
import asyncio
import datetime
import uuid
from typing import List, Dict, Any, Optional, Tuple, Set
from sentence_transformers import SentenceTransformer
from elasticsearch import AsyncElasticsearch
from confluent_kafka import Consumer, Producer, KafkaException
from pydantic import BaseModel
from config import KAFKA_CONFIG,ES_CONFIG
from libaryLanguage import LibraryLanguageDetector



# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataStorageDto(BaseModel):
    tenantId: str
    storedIds: Set[str]
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

class Chunk(BaseModel):
    text: str
    section: str
    type: str
    metadata: ChunkMetadata

class MultilingualMessageProcessor:
    
    def __init__(self, es_config: Dict[str, Any], kafka_config: Dict[str, Any], model_path: Optional[str] = None):
        """
        Initialize the multilingual message processor with support for 70+ languages.
        
        Args:
            es_config: Elasticsearch configuration.
            kafka_config: Kafka configuration.
            model_path: Optional path to a local SentenceTransformer model.
        """
        self.es_config = es_config
        self.kafka_config = kafka_config
        
        # Initialize SentenceTransformer with multilingual model
        model_name = 'paraphrase-multilingual-mpnet-base-v2'
        try:
            if model_path:
                logger.info(f"Loading model from local path: {model_path}")
                self.st_model = SentenceTransformer(model_path)
            else:
                logger.info(f"Loading model {model_name} from Hugging Face")
                self.st_model = SentenceTransformer(model_name)
        except Exception as e:
            logger.error(f"Error loading sentence transformer model: {e}")
            raise
            
        # Initialize async Elasticsearch client
        self.es_client = AsyncElasticsearch(
            es_config['hosts'],
            basic_auth=(es_config.get('username', ''), es_config.get('password', '')),
            retry_on_timeout=True,
            max_retries=3
        )
        
        # Kafka configuration
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
        
        # Initialize Kafka producer
        self.producer = None

        self.libraryDetector = LibraryLanguageDetector()

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

    def identify_document_sections(self, text: str, language: str) -> List[Tuple[str, str]]:
        """
        Identify document sections with language-specific patterns.
        
        Args:
            text: Document text
            language: Language code
            
        Returns:
            List of (section_title, section_content) tuples
        """
        script_group = self.get_script_group(language)
        
        # Try Markdown-style headers first (universal format)
        markdown_patterns = [
            # Level 2 headers with content
            (r'(?:^|\n)##\s+(.*?)(?:\n|$)((?:.|\n)*?)(?=\n##|\Z)', True),
            # Level 1 headers with content
            (r'(?:^|\n)#\s+(.*?)(?:\n|$)((?:.|\n)*?)(?=\n#|\Z)', True)
        ]
        
        for pattern, is_full_match in markdown_patterns:
            matches = re.finditer(pattern, text, re.MULTILINE | re.DOTALL)
            sections = [(m.group(1).strip(), m.group(2).strip()) for m in matches]
            if sections:
                return sections
        
        # Try language-specific section headers
        section_terms = self.get_section_headers(language)
        
        # Create patterns for each section term
        section_patterns = []
        for term in section_terms:
            if script_group in ['latin', 'cyrillic', 'greek']:
                # For Latin/Cyrillic/Greek scripts, look for capitalized headers
                pattern = rf'(?:^|\n)(?:{term}|{term.capitalize()})\s*\d*[\.\:]\s*(.*?)(?:\n|$)'
                section_patterns.append(pattern)
            else:
                # For other scripts, just look for the term
                pattern = rf'(?:^|\n){term}\s*\d*[\.\:]\s*(.*?)(?:\n|$)'
                section_patterns.append(pattern)
        
        # Also include numbered headers
        if script_group == 'cjk':
            # CJK numbering
            numbered_header = r'(?:^|\n)(?:[一二三四五六七八九十]+[\.．、]|[0-9]+[\.．、])\s*([^\n]+)'
        elif script_group in ['devanagari', 'bengali', 'dravidian', 'gurmukhi', 'gujarati']:
            # Indic numbering or Latin numbers
            numbered_header = r'(?:^|\n)(?:\d+[\.।]|[१२३४५६७८९०]+[\.।])\s*([^\n]+)'
        elif script_group == 'arabic':
            # Arabic/Persian numbering
            numbered_header = r'(?:^|\n)(?:\d+[\.،]|[١٢٣٤٥٦٧٨٩٠]+[\.،])\s*([^\n]+)'
        else:
            # Default Latin numbering
            numbered_header = r'(?:^|\n)\d+[\.]\s*([^\n]+)'
            
        section_patterns.append(numbered_header)
        
        # Try to find sections using the patterns
        for pattern in section_patterns:
            headers = re.finditer(pattern, text)
            header_positions = [(m.start(), m.group(1).strip()) for m in headers]
            
            if len(header_positions) > 0:
                sections = []
                for i in range(len(header_positions)):
                    start_pos, header = header_positions[i]
                    end_pos = header_positions[i+1][0] if i+1 < len(header_positions) else len(text)
                    
                    # Extract the section content, excluding the header itself
                    header_line_end = text.find('\n', start_pos + 1)
                    if header_line_end == -1:
                        header_line_end = len(text)
                    
                    content = text[header_line_end:end_pos].strip()
                    sections.append((header, content))
                    
                if sections:
                    return sections
        
        # If no sections found, look for visual separators
        separator_patterns = [
            r'\n-{3,}\n',  # Markdown-style separators
            r'\n\*{3,}\n',  # Asterisk separators
            r'\n={3,}\n',   # Equals separators
            r'\n\n\n+'      # Multiple blank lines
        ]
        
        for pattern in separator_patterns:
            parts = re.split(pattern, text)
            if len(parts) > 1:
                sections = []
                for part in parts:
                    part = part.strip()
                    if not part:
                        continue
                    
                    # Try to use the first line as a header
                    lines = part.split('\n')
                    if len(lines) > 1 and len(lines[0].strip()) < 100:  # Reasonable header length
                        header = lines[0].strip()
                        content = '\n'.join(lines[1:]).strip()
                        sections.append((header, content))
                    else:
                        # No clear header, use "Section N"
                        sections.append((f"Section {len(sections)+1}", part))
                        
                if sections:
                    return sections
        
        # If still no sections, just return the whole text as one section
        return [("General", text)]

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
        
        Args:
            text: Document text
            language: Language code
            
        Returns:
            List of Chunk objects
        """
        # Detect document type
        doc_type = self.detect_document_type(text, language)
        logger.info(f"doc_type: {doc_type}")
        chunks = []
        
        # Identify document sections
        sections = self.identify_document_sections(text, language)
        
        # Process each section
        for section_idx, (section_title, section_content) in enumerate(sections):
            # Determine appropriate processing for this section
            if doc_type == 'faq' or any(term in section_title.lower() for term in self.get_faq_terms(language)):
                # Process as FAQ section
                qa_pairs = self.extract_qa_pairs(section_content, language)
                
                if qa_pairs:
                    for i, (question, answer) in enumerate(qa_pairs):
                        chunks.append(Chunk(
                            text=f"Q: {question}\nA: {answer}",
                            section=section_title,
                            type="qa_pair",
                            metadata=ChunkMetadata(
                                hasQuestion=True,
                                documentType='faq',
                                language=language,
                                qaFormat="explicit",
                                question=question,
                                chunkIndex=len(chunks),
                                totalChunks=0  # Will update later
                            )
                        ))
                else:
                    # No Q&A pairs found, process as regular text
                    content_chunks = self.chunk_text(section_content, language)
                    
                    for i, chunk_text in enumerate(content_chunks):
                        chunks.append(Chunk(
                            text=chunk_text,
                            section=section_title,
                            type="content",
                            metadata=ChunkMetadata(
                                hasQuestion=False,
                                documentType=doc_type,
                                language=language,
                                chunkIndex=len(chunks),
                                totalChunks=0  # Will update later
                            )
                        ))
            elif doc_type == 'policy' or any(term in section_title.lower() for term in self.get_policy_terms(language)):
                # Process as policy section
                
                # Look for numbered clauses
                script_group = self.get_script_group(language)
                if script_group == 'cjk':
                    # For CJK
                    clause_pattern = r'(?:^|\n)\s*(?:\d+|[一二三四五六七八九十]+)[\.．、]\s*(.*?)(?=\n\s*(?:\d+|[一二三四五六七八九十]+)[\.．、]|\Z)'
                elif script_group in ['devanagari', 'bengali', 'dravidian', 'gurmukhi', 'gujarati']:
                    # For Indic scripts
                    clause_pattern = r'(?:^|\n)\s*(?:\d+|[१२३४५६७८९०]+)[\.।]\s*(.*?)(?=\n\s*(?:\d+|[१२३४५६७८९०]+)[\.।]|\Z)'
                elif script_group == 'arabic':
                    # For Arabic script
                    clause_pattern = r'(?:^|\n)\s*(?:\d+|[١٢٣٤٥٦٧٨٩٠]+)[\.،]\s*(.*?)(?=\n\s*(?:\d+|[١٢٣٤٥٦٧٨٩٠]+)[\.،]|\Z)'
                else:
                    # Default pattern 
                    clause_pattern = r'(?:^|\n)\s*\d+\.\s*(.*?)(?=\n\s*\d+\.|\Z)'
                
                clauses = []
                for match in re.finditer(clause_pattern, section_content, re.DOTALL):
                    clauses.append(match.group(1).strip())
                
                if clauses:
                    # Process clauses
                    current_clauses = []
                    current_size = 0
                    max_size = 300  # max words per chunk
                    
                    for clause in clauses:
                        clause_size = len(clause.split())
                        
                        if current_size + clause_size > max_size and current_clauses:
                            # Save current chunk
                            chunk_text = "\n\n".join(current_clauses)
                            chunks.append(Chunk(
                                text=chunk_text,
                                section=section_title,
                                type="policy_clauses",
                                metadata=ChunkMetadata(
                                    hasQuestion=False,
                                    documentType='policy',
                                    language=language,
                                    chunkIndex=len(chunks),
                                    totalChunks=0  # Will update later
                                )
                            ))
                            current_clauses = [clause]
                            current_size = clause_size
                        else:
                            current_clauses.append(clause)
                            current_size += clause_size
                    
                    # Add remaining clauses
                    if current_clauses:
                        chunk_text = "\n\n".join(current_clauses)
                        chunks.append(Chunk(
                            text=chunk_text,
                            section=section_title,
                            type="policy_clauses",
                            metadata=ChunkMetadata(
                                hasQuestion=False,
                                documentType='policy',
                                language=language,
                                chunkIndex=len(chunks),
                                totalChunks=0  # Will update later
                            )
                        ))
                else:
                    # No clauses found, process as regular text
                    content_chunks = self.chunk_text(section_content, language)
                    
                    for i, chunk_text in enumerate(content_chunks):
                        chunks.append(Chunk(
                            text=chunk_text,
                            section=section_title,
                            type="policy_content",
                            metadata=ChunkMetadata(
                                hasQuestion=False,
                                documentType='policy',
                                language=language,
                                chunkIndex=len(chunks),
                                totalChunks=0  # Will update later
                            )
                        ))
            else:
                # Process as general content
                content_chunks = self.chunk_text(section_content, language)
                
                for i, chunk_text in enumerate(content_chunks):
                    chunks.append(Chunk(
                        text=chunk_text,
                        section=section_title,
                        type="content",
                        metadata=ChunkMetadata(
                            hasQuestion=False,
                            documentType=doc_type,
                            language=language,
                            chunkIndex=len(chunks),
                            totalChunks=0  # Will update later
                        )
                    ))
        
        # Update total chunks count
        total_chunks = len(chunks)
        for chunk in chunks:
            chunk.metadata.totalChunks = total_chunks
            
        return chunks

    def chunk_text(self, text: str, language: str, max_chunk_size: int = 300) -> List[str]:
        """
        Split text into appropriate sized chunks with language awareness.
        
        Args:
            text: Text to chunk
            language: Language code
            max_chunk_size: Maximum words per chunk
            
        Returns:
            List of text chunks
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
        
        # Ensure we don't have empty chunks
        return [chunk for chunk in chunks if chunk.strip()]

    async def _setup_elasticsearch_indices(self):
        """Setup Elasticsearch indices with proper mappings for vector search"""
 
        # Chunks index with vector field
        chunks_index = self.es_config['tenant_document_index_name']
        exists = await self.es_client.indices.exists(index=chunks_index)
        if not exists:
            await self.es_client.indices.create(
                index=chunks_index,
                mappings={
                    "properties": {
                        "content": {"type": "text"},
                        "contentVector": {
                            "type": "dense_vector",
                            "dims": 768,  # mpnet-base-v2 has 768 dimensions
                            "index": True,
                            "similarity": "cosine"
                        },
                        "tenantId": {"type": "keyword"},
                        "documentId": {"type": "keyword"},
                        "chunkType": {"type": "keyword"},
                        "sectionTitle": {"type": "text"},
                        "metadata": {"type": "object", "enabled": True}
                    }
                }
            )
            logger.info(f"Created chunks index: {chunks_index}")

    async def process_document_message(self, message: Dict[str, Any]):
        """
        Process an individual message and store it in Elasticsearch with vector embeddings.
        
        Args:
            message: Message dictionary containing content and metadata.
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
        
        # Process and index each chunk
        indexing_tasks = []
        for i, chunk in enumerate(chunks):
            # Generate vector embedding for the chunk
            vector = self.st_model.encode(chunk.text).tolist()
            
            # Add any additional metadata from the message
            combined_metadata = {**metadata}
            combined_metadata.update(chunk.metadata.model_dump())
            combined_metadata['chunkIndex'] = i
            combined_metadata['totalChunks'] = len(chunks)
            
            # Prepare chunk document
            chunk_doc = {
                'content': chunk.text,
                'contentVector': vector,
                'tenantId': tenant_id,
                'documentId': document_id,
                'chunkType': chunk.type,
                'sectionTitle': chunk.section,
                'metadata': combined_metadata
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
        
        # Delete from both indices
        await self.es_client.delete_by_query(
            index=self.es_config['tenant_document_index_name'], 
            body=delete_query
        )

        
        logger.info(f"Deleted old inactive documents for tenant_id: {tenant_id}")
        
        # Send success response
        await self.publish_kafka_message(
            self.response_topic,
            tenant_id,
            DataStorageDto(
                tenantId=tenant_id,
                storedIds= set(document_id),
                isDone= True,
                dataType='doc'
            )
        )
        
        logger.info(f"Successfully indexed all {len(chunks)} chunks for document {document_id}")


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
                storedIds=processed_faq_ids,
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
            serialized_value = json.dumps(message.model_dump()).encode("utf-8")
        else:
            serialized_value = json.dumps(message).encode("utf-8")
        
        # Create an asyncio Future to wait for delivery report
        future = asyncio.Future()
        
        def delivery_callback(err, msg):
            if err:
                future.set_exception(Exception(f"Message delivery failed: {err} for key: {key}"))
            else:
                future.set_result(msg)
        
        self.producer.produce(
            topic,
            key=serialized_key,
            value=serialized_value,
            callback=delivery_callback
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
            "original_message": message,
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



    async def run(self):
        """Main processing loop."""
        logger.info("Starting multilingual message processor...")
        
        # Initialize producer
        self.producer = Producer(self.producer_config)
        logger.info("Kafka producer initialized successfully")
        
        # Setup Elasticsearch indices
        await self._setup_elasticsearch_indices()
        
        # Create separate methods for the consumer loops without their own exception handling
        async def document_consumer_loop():
            consumer = Consumer(self.consumer_config)
            consumer.subscribe([self.document_request_topic])
            logger.info(f"Subscribed to topic: {self.document_request_topic}")
            
            try:
                while True:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        await asyncio.sleep(0.1)  # Small delay to prevent CPU spinning
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaException._PARTITION_EOF:
                            logger.info(f"Reached end of partition {msg.partition()}")
                        else:
                            logger.error(f"Error: {msg.error()}")
                        continue
                    
                    # Process message
                    try:
                        value = self._parse_message(msg.value())
                        logger.info(f"Received document message from partition {msg.partition()}, offset {msg.offset()}")
                        await self.process_document_message(value)
                        
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}", exc_info=True)
                        await self.send_to_dead_letter_queue(
                            self.document_dlq_topic,
                            self._parse_message(msg.value()) if msg.value() else {},
                            str(e)
                        )
            finally:
                consumer.close()
                logger.info("Document Kafka consumer closed")
        
        async def faq_consumer_loop():
            consumer = Consumer(self.consumer_config)
            consumer.subscribe([self.faq_request_topic])
            logger.info(f"Subscribed to topic: {self.faq_request_topic}")
            
            try:
                while True:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        await asyncio.sleep(0.1)  # Small delay to prevent CPU spinning
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaException._PARTITION_EOF:
                            logger.info(f"Reached end of partition {msg.partition()}")
                        else:
                            logger.error(f"Error: {msg.error()}")
                        continue
                    
                    # Process message
                    try:
                        value = self._parse_message(msg.value())
                        logger.info(f"Received FAQ message from partition {msg.partition()}, offset {msg.offset()}")
                        await self.process_faq_message(value)
                        
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}", exc_info=True)
                        await self.send_to_dead_letter_queue(
                            self.faq_dlq_topic,
                            self._parse_message(msg.value()) if msg.value() else {},
                            str(e)
                        )
            finally:
                consumer.close()
                logger.info("FAQ Kafka consumer closed")
        
        # Start consuming messages concurrently with a single exception handler
        try:
            await asyncio.gather(
                document_consumer_loop(),
                faq_consumer_loop()
            )
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, shutting down...")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {str(e)}", exc_info=True)
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Graceful shutdown of resources."""
        logger.info("Shutting down...")
        
        # Close the Elasticsearch client
        await self.es_client.close()
        
        # Ensure all messages are delivered before shutting down producer
        if self.producer:
            self.producer.flush()
        
        logger.info("Resources closed.")




# Example usage
if __name__ == "__main__":
    # Initialize processor
    processor = MultilingualMessageProcessor(ES_CONFIG, KAFKA_CONFIG)
    
    # Run the processor
    asyncio.run(processor.run())