# language_constants.py
from typing import Dict, List, Tuple

class LanguageConstants:
    """Language-specific constants and configurations"""
    
    # Define language script groupings
    SCRIPT_GROUPS = {
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
    SENTENCE_END_MARKERS = {
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
    SECTION_HEADERS = {
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

    FAQ_TERMS = {
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
        'bs': ['často postavljana pitanja', 'pitanja i odgovori'],
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

    POLICY_TERMS = {
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

    QA_MARKERS = {
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

    # Localized words for common terms
    PART_WORDS = {
        'en': 'Part', 'es': 'Parte', 'fr': 'Partie', 'de': 'Teil',
        'it': 'Parte', 'pt': 'Parte', 'ru': 'Часть', 'zh': '部分',
        'ja': 'パート', 'ko': '부분', 'ar': 'جزء', 'hi': 'भाग',
        'th': 'ส่วน', 'he': 'חלק', 'bn': 'অংশ', 'ta': 'பகுதি'
    }

    SECTION_WORDS = {
        'en': 'Section', 'es': 'Sección', 'fr': 'Section', 'de': 'Abschnitt',
        'it': 'Sezione', 'pt': 'Seção', 'ru': 'Раздел', 'zh': '章节',
        'ja': 'セクション', 'ko': '섹션', 'ar': 'قسم', 'hi': 'खंड',
        'th': 'ส่วน', 'he': 'סעיף', 'bn': 'বিভাগ', 'ta': 'பிரிவு'
    }

    # Add these constants to your LanguageConstants class

    PRICING_TERMS = {
        # Default/English
        'en': ['pricing', 'price', 'cost', 'fee', 'rate', 'tariff', 'subscription', 'plan', 'billing'],
        
        # Latin script European languages
        'de': ['preise', 'preis', 'kosten', 'gebühr', 'tarif', 'abonnement', 'plan', 'abrechnung'],
        'es': ['precios', 'precio', 'costo', 'tarifa', 'cuota', 'suscripción', 'plan', 'facturación'],
        'fr': ['prix', 'coût', 'tarif', 'frais', 'abonnement', 'plan', 'facturation'],
        'it': ['prezzi', 'prezzo', 'costo', 'tariffa', 'quota', 'abbonamento', 'piano', 'fatturazione'],
        'pt': ['preços', 'preço', 'custo', 'tarifa', 'taxa', 'assinatura', 'plano', 'faturamento'],
        'nl': ['prijzen', 'prijs', 'kosten', 'tarief', 'abonnement', 'plan', 'facturering'],
        'pl': ['ceny', 'cena', 'koszt', 'taryfa', 'opłata', 'subskrypcja', 'plan', 'rozliczenia'],
        'ro': ['prețuri', 'preț', 'cost', 'tarif', 'taxă', 'abonament', 'plan', 'facturare'],
        'sv': ['priser', 'pris', 'kostnad', 'avgift', 'prenumeration', 'plan', 'fakturering'],
        'no': ['priser', 'pris', 'kostnad', 'avgift', 'abonnement', 'plan', 'fakturering'],
        'da': ['priser', 'pris', 'omkostning', 'gebyr', 'abonnement', 'plan', 'fakturering'],
        'fi': ['hinnat', 'hinta', 'kustannus', 'maksu', 'tilaus', 'suunnitelma', 'laskutus'],
        'hu': ['árak', 'ár', 'költség', 'díj', 'előfizetés', 'terv', 'számlázás'],
        'cs': ['ceny', 'cena', 'náklady', 'poplatek', 'předplatné', 'plán', 'fakturace'],
        'sk': ['ceny', 'cena', 'náklady', 'poplatok', 'predplatné', 'plán', 'fakturácia'],
        'sl': ['cene', 'cena', 'stroški', 'pristojbina', 'naročnina', 'načrt', 'obračun'],
        'hr': ['cijene', 'cijena', 'troškovi', 'naknada', 'pretplata', 'plan', 'naplata'],
        'bs': ['cijene', 'cijena', 'troškovi', 'naknada', 'pretplata', 'plan', 'naplata'],
        'tr': ['fiyatlar', 'fiyat', 'maliyet', 'ücret', 'abonelik', 'plan', 'faturalama'],
        
        # Cyrillic script languages
        'ru': ['цены', 'цена', 'стоимость', 'тариф', 'подписка', 'план', 'выставление счетов'],
        'uk': ['ціни', 'ціна', 'вартість', 'тариф', 'підписка', 'план', 'виставлення рахунків'],
        'bg': ['цени', 'цена', 'разходи', 'тарифа', 'абонамент', 'план', 'фактуриране'],
        'mk': ['цени', 'цена', 'трошоци', 'тарифа', 'претплата', 'план', 'фактурирање'],
        'sr-cyr': ['цене', 'цена', 'трошкови', 'тарифа', 'претплата', 'план', 'наплата'],
        'kk': ['бағалар', 'баға', 'құн', 'тариф', 'жазылым', 'жоспар', 'есеп беру'],
        
        # Hindi and other Indic languages
        'hi': ['मूल्य', 'कीमत', 'लागत', 'शुल्क', 'दर', 'सब्सक्रिप्शन', 'योजना', 'बिलिंग'],
        'mr': ['किंमती', 'किंमत', 'खर्च', 'शुल्क', 'दर', 'सबस्क्रिप्शन', 'योजना', 'बिलिंग'],
        'ne': ['मूल्यहरू', 'मूल्य', 'लागत', 'शुल्क', 'दर', 'सदस्यता', 'योजना', 'बिलिङ'],
        'bn': ['দাম', 'মূল্য', 'খরচ', 'ফি', 'হার', 'সাবস্ক্রিপশন', 'পরিকল্পনা', 'বিলিং'],
        'pa': ['ਕੀਮਤਾਂ', 'ਕੀਮਤ', 'ਲਾਗਤ', 'ਫੀਸ', 'ਦਰ', 'ਸਬਸਕ੍ਰਿਪਸ਼ਨ', 'ਯੋਜਨਾ', 'ਬਿਲਿੰਗ'],
        'gu': ['કિંમતો', 'કિંમત', 'ખર્ચ', 'ફી', 'દર', 'સબ્સ્ક્રિપ્શન', 'યોજના', 'બિલિંગ'],
        'si': ['මිල', 'වටිනාකම', 'පිරිවැය', 'ගාස්තු', 'අනුපාතය', 'දායකත්වය', 'සැලසුම', 'බිල්පත්'],
        'ta': ['விலைகள்', 'விலை', 'செலவு', 'கட்டணம்', 'விகிதம்', 'சந்தா', 'திட்டம்', 'பில்லிங்'],
        'te': ['ధరలు', 'ధర', 'ఖర్చు', 'రుసుము', 'రేటు', 'సబ్స్క్రిప్షన్', 'ప్లాన్', 'బిల్లింగ్'],
        'kn': ['ಬೆಲೆಗಳು', 'ಬೆಲೆ', 'ವೆಚ್ಚ', 'ಶುಲ್ಕ', 'ದರ', 'ಚಂದಾದಾರಿಕೆ', 'ಯೋಜನೆ', 'ಬಿಲ್ಲಿಂಗ್'],
        'ml': ['വിലകൾ', 'വില', 'ചെലവ്', 'ഫീസ്', 'നിരക്ക്', 'സബ്സ്ക്രിപ്ഷൻ', 'പ്ലാൻ', 'ബിലിങ്'],
        
        # Middle Eastern languages
        'ar': ['أسعار', 'سعر', 'تكلفة', 'رسوم', 'معدل', 'اشتراك', 'خطة', 'فواتير'],
        'fa': ['قیمت‌ها', 'قیمت', 'هزینه', 'کارمزد', 'نرخ', 'اشتراک', 'طرح', 'صورتحساب'],
        'ur': ['قیمتیں', 'قیمت', 'لاگت', 'فیس', 'شرح', 'سبسکرپشن', 'پلان', 'بلنگ'],
        'ps': ['بیې', 'بیه', 'لګښت', 'فیس', 'نرخ', 'غړیتوب', 'پلان', 'بیل کول'],
        'he': ['מחירים', 'מחיר', 'עלות', 'עמלה', 'תעריף', 'מנוי', 'תוכנית', 'חיוב'],
        
        # East Asian languages
        'zh': ['价格', '定价', '成本', '费用', '费率', '订阅', '计划', '计费'],
        'zh-tw': ['價格', '定價', '成本', '費用', '費率', '訂閱', '計劃', '計費'],
        'ja': ['価格', '料金', 'コスト', '手数料', 'レート', 'サブスクリプション', 'プラン', '請求'],
        'ko': ['가격', '요금', '비용', '수수료', '요율', '구독', '플랜', '청구'],
        
        # Thai
        'th': ['ราคา', 'ค่าใช้จ่าย', 'ค่าธรรมเนียม', 'อัตรา', 'การสมัครสมาชิก', 'แผน', 'การเรียกเก็บเงิน'],
        
        # Greek
        'el': ['τιμές', 'τιμή', 'κόστος', 'τέλος', 'συνδρομή', 'σχέδιο', 'χρέωση'],
        
        # Indonesian and Malay
        'id': ['harga', 'biaya', 'tarif', 'biaya berlangganan', 'paket', 'penagihan'],
        'ms': ['harga', 'kos', 'kadar', 'langganan', 'pelan', 'pengebilan'],
        
        # Vietnamese
        'vi': ['giá', 'giá cả', 'chi phí', 'phí', 'tỷ lệ', 'đăng ký', 'gói', 'thanh toán'],
        
        # African languages
        'sw': ['bei', 'gharama', 'ada', 'kiwango', 'uanachama', 'mpango', 'malipo'],
        'ha': ['farashi', 'kudade', 'kudin', 'kaso', 'shirin', 'tsari', 'biyan kudi'],
        'ig': ['ọnụ ahịa', 'ọnụ ahịa', 'ọnụ ego', 'ụgwọ', 'atụmatụ', 'ịkwụ ụgwọ']
    }

    FEATURE_TERMS = {
        # Default/English
        'en': ['features', 'functionality', 'capabilities', 'functions', 'tools', 'options', 'benefits'],
        
        # Latin script European languages
        'de': ['funktionen', 'merkmale', 'eigenschaften', 'fähigkeiten', 'werkzeuge', 'optionen', 'vorteile'],
        'es': ['características', 'funciones', 'funcionalidad', 'capacidades', 'herramientas', 'opciones', 'beneficios'],
        'fr': ['fonctionnalités', 'caractéristiques', 'fonctions', 'capacités', 'outils', 'options', 'avantages'],
        'it': ['funzionalità', 'caratteristiche', 'funzioni', 'capacità', 'strumenti', 'opzioni', 'vantaggi'],
        'pt': ['recursos', 'funcionalidades', 'características', 'funções', 'ferramentas', 'opções', 'benefícios'],
        'nl': ['functies', 'functionaliteit', 'eigenschappen', 'mogelijkheden', 'gereedschappen', 'opties', 'voordelen'],
        'pl': ['funkcje', 'funkcjonalność', 'cechy', 'możliwości', 'narzędzia', 'opcje', 'korzyści'],
        'ro': ['funcții', 'funcționalitate', 'caracteristici', 'capabilități', 'unelte', 'opțiuni', 'beneficii'],
        'sv': ['funktioner', 'funktionalitet', 'egenskaper', 'möjligheter', 'verktyg', 'alternativ', 'fördelar'],
        'no': ['funksjoner', 'funksjonalitet', 'egenskaper', 'muligheter', 'verktøy', 'alternativer', 'fordeler'],
        'da': ['funktioner', 'funktionalitet', 'egenskaber', 'muligheder', 'værktøjer', 'muligheder', 'fordele'],
        'fi': ['ominaisuudet', 'toiminnallisuus', 'toiminnot', 'mahdollisuudet', 'työkalut', 'vaihtoehdot', 'edut'],
        'hu': ['funkciók', 'funkcionalitás', 'jellemzők', 'képességek', 'eszközök', 'lehetőségek', 'előnyök'],
        'cs': ['funkce', 'funkcionalita', 'vlastnosti', 'schopnosti', 'nástroje', 'možnosti', 'výhody'],
        'sk': ['funkcie', 'funkcionalita', 'vlastnosti', 'schopnosti', 'nástroje', 'možnosti', 'výhody'],
        'sl': ['funkcije', 'funkcionalnost', 'lastnosti', 'zmožnosti', 'orodja', 'možnosti', 'prednosti'],
        'hr': ['funkcije', 'funkcionalnost', 'značajke', 'sposobnosti', 'alati', 'opcije', 'prednosti'],
        'bs': ['funkcije', 'funkcionalnost', 'značajke', 'sposobnosti', 'alati', 'opcije', 'prednosti'],
        'tr': ['özellikler', 'fonksiyonalite', 'işlevler', 'yetenekler', 'araçlar', 'seçenekler', 'faydalar'],
        
        # Cyrillic script languages
        'ru': ['функции', 'функциональность', 'возможности', 'способности', 'инструменты', 'опции', 'преимущества'],
        'uk': ['функції', 'функціональність', 'можливості', 'здібності', 'інструменти', 'опції', 'переваги'],
        'bg': ['функции', 'функционалност', 'възможности', 'способности', 'инструменти', 'опции', 'предимства'],
        'mk': ['функции', 'функционалност', 'можности', 'способности', 'алатки', 'опции', 'предности'],
        'sr-cyr': ['функције', 'функционалност', 'могућности', 'способности', 'алати', 'опције', 'предности'],
        'kk': ['функциялар', 'функционалдық', 'мүмкіндіктер', 'қабілеттер', 'құралдар', 'опциялар', 'артықшылықтар'],
        
        # Hindi and other Indic languages
        'hi': ['सुविधाएं', 'कार्यक्षमता', 'विशेषताएं', 'क्षमताएं', 'उपकरण', 'विकल्प', 'लाभ'],
        'mr': ['वैशिष्ट्ये', 'कार्यक्षमता', 'गुणधर्म', 'क्षमता', 'साधने', 'पर्याय', 'फायदे'],
        'ne': ['सुविधाहरू', 'कार्यक्षमता', 'विशेषताहरू', 'क्षमताहरू', 'उपकरणहरू', 'विकल्पहरू', 'फाइदाहरू'],
        'bn': ['বৈশিষ্ট্যসমূহ', 'কার্যকারিতা', 'গুণাবলী', 'সক্ষমতা', 'সরঞ্জাম', 'বিকল্প', 'সুবিধা'],
        'pa': ['ਫੀਚਰਸ', 'ਕਾਰਜਸ਼ੀਲਤਾ', 'ਵਿਸ਼ੇਸ਼ਤਾਵਾਂ', 'ਸਮਰੱਥਾਵਾਂ', 'ਸਾਧਨ', 'ਵਿਕਲਪ', 'ਲਾਭ'],
        'gu': ['સુવિધાઓ', 'કાર્યક્ષમતા', 'લક્ષણો', 'ક્ષમતાઓ', 'સાધનો', 'વિકલ્પો', 'ફાયદા'],
        'si': ['විශේෂාංග', 'ක්‍රියාකාරිත්වය', 'ලක්ෂණ', 'හැකියාවන්', 'මෙවලම්', 'විකල්ප', 'ප්‍රතිලාභ'],
        'ta': ['அம்சங்கள்', 'செயல்பாடு', 'பண்புகள்', 'திறன்கள்', 'கருவிகள்', 'விருப்பங்கள்', 'நன்மைகள்'],
        'te': ['లక్షణలు', 'కార్యాచరణ', 'లక్షణాలు', 'సామర్థ్యాలు', 'సాధనాలు', 'ఎంపికలు', 'ప్రయోజనాలు'],
        'kn': ['ವೈಶಿಷ್ಟ್ಯಗಳು', 'ಕಾರ್ಯನಿರ್ವಹಣೆ', 'ಗುಣಲಕ್ಷಣಗಳು', 'ಸಾಮರ್ಥ್ಯಗಳು', 'ಉಪಕರಣಗಳು', 'ಆಯ್ಕೆಗಳು', 'ಪ್ರಯೋಜನಗಳು'],
        'ml': ['സവിശേഷതകൾ', 'പ്രവർത്തനം', 'സവിശേഷതകൾ', 'കഴിവുകൾ', 'ഉപകരണങ്ങൾ', 'ഓപ്ഷനുകൾ', 'ഗുണങ്ങൾ'],
        
        # Middle Eastern languages
        'ar': ['ميزات', 'وظائف', 'خصائص', 'قدرات', 'أدوات', 'خيارات', 'فوائد'],
        'fa': ['ویژگی‌ها', 'عملکرد', 'خصوصیات', 'قابلیت‌ها', 'ابزارها', 'گزینه‌ها', 'مزایا'],
        'ur': ['خصوصیات', 'فعالیت', 'خصوصیات', 'صلاحیات', 'ٹولز', 'اختیارات', 'فوائد'],
        'ps': ['ځانګړتیاوې', 'فعالیت', 'ځانګړنې', 'وړتیاوې', 'وسایل', 'انتخابونه', 'ګټې'],
        'he': ['תכונות', 'פונקציונליות', 'יכולות', 'כלים', 'אפשרויות', 'יתרונות'],
        
        # East Asian languages
        'zh': ['功能', '特性', '特征', '能力', '工具', '选项', '优势'],
        'zh-tw': ['功能', '特性', '特徵', '能力', '工具', '選項', '優勢'],
        'ja': ['機能', '特徴', '機能性', '能力', 'ツール', 'オプション', 'メリット'],
        'ko': ['기능', '특징', '기능성', '능력', '도구', '옵션', '장점'],
        
        # Thai
        'th': ['ฟีเจอร์', 'ฟังก์ชัน', 'คุณสมบัติ', 'ความสามารถ', 'เครื่องมือ', 'ตัวเลือก', 'ประโยชน์'],
        
        # Greek
        'el': ['χαρακτηριστικά', 'λειτουργικότητα', 'δυνατότητες', 'εργαλεία', 'επιλογές', 'πλεονεκτήματα'],
        
        # Indonesian and Malay
        'id': ['fitur', 'fungsionalitas', 'karakteristik', 'kemampuan', 'alat', 'pilihan', 'manfaat'],
        'ms': ['ciri', 'kefungsian', 'ciri-ciri', 'keupayaan', 'alat', 'pilihan', 'faedah'],
        
        # Vietnamese
        'vi': ['tính năng', 'chức năng', 'đặc điểm', 'khả năng', 'công cụ', 'tùy chọn', 'lợi ích'],
        
        # African languages
        'sw': ['vipengele', 'utendaji', 'sifa', 'uwezo', 'vifaa', 'chaguo', 'faida'],
        'ha': ['fasaloli', 'aiki', 'halaye', 'iyawa', 'kayan aiki', 'zaɓuɓɓuka', 'fa\'ida'],
        'ig': ['atụmatụ', 'arụ ọrụ', 'àgwà', 'ike', 'ngwa ọrụ', 'nhọrọ', 'uru']
    }

    OVERVIEW_TERMS = {
        # Default/English
        'en': ['overview', 'introduction', 'summary', 'about', 'general', 'basics', 'getting started'],
        
        # Latin script European languages
        'de': ['überblick', 'einführung', 'zusammenfassung', 'über', 'allgemein', 'grundlagen', 'erste schritte'],
        'es': ['resumen', 'introducción', 'sumario', 'acerca de', 'general', 'conceptos básicos', 'primeros pasos'],
        'fr': ['aperçu', 'introduction', 'résumé', 'à propos', 'général', 'bases', 'premiers pas'],
        'it': ['panoramica', 'introduzione', 'riassunto', 'informazioni', 'generale', 'nozioni di base', 'primi passi'],
        'pt': ['visão geral', 'introdução', 'resumo', 'sobre', 'geral', 'básico', 'primeiros passos'],
        'nl': ['overzicht', 'inleiding', 'samenvatting', 'over', 'algemeen', 'basisprincipes', 'aan de slag'],
        'pl': ['przegląd', 'wprowadzenie', 'podsumowanie', 'o nas', 'ogólne', 'podstawy', 'pierwsze kroki'],
        'ro': ['prezentare generală', 'introducere', 'rezumat', 'despre', 'general', 'noțiuni de bază', 'primii pași'],
        'sv': ['översikt', 'introduktion', 'sammanfattning', 'om', 'allmänt', 'grundläggande', 'komma igång'],
        'no': ['oversikt', 'introduksjon', 'sammendrag', 'om', 'generelt', 'grunnleggende', 'kom i gang'],
        'da': ['oversigt', 'introduktion', 'sammendrag', 'om', 'generelt', 'grundlæggende', 'kom i gang'],
        'fi': ['yleiskatsaus', 'johdanto', 'yhteenveto', 'tietoja', 'yleinen', 'perusteet', 'aloittaminen'],
        'hu': ['áttekintés', 'bevezetés', 'összefoglaló', 'rólunk', 'általános', 'alapok', 'kezdeti lépések'],
        'cs': ['přehled', 'úvod', 'shrnutí', 'o nás', 'obecné', 'základy', 'první kroky'],
        'sk': ['prehľad', 'úvod', 'zhrnutie', 'o nás', 'všeobecné', 'základy', 'prvé kroky'],
        'sl': ['pregled', 'uvod', 'povzetek', 'o nas', 'splošno', 'osnove', 'prvi koraki'],
        'hr': ['pregled', 'uvod', 'sažetak', 'o nama', 'općenito', 'osnove', 'prvi koraci'],
        'bs': ['pregled', 'uvod', 'sažetak', 'o nama', 'općenito', 'osnove', 'prvi koraci'],
        'tr': ['genel bakış', 'giriş', 'özet', 'hakkında', 'genel', 'temel bilgiler', 'başlangıç'],
        
        # Cyrillic script languages
        'ru': ['обзор', 'введение', 'резюме', 'о нас', 'общий', 'основы', 'начало работы'],
        'uk': ['огляд', 'вступ', 'резюме', 'про нас', 'загальний', 'основи', 'початок роботи'],
        'bg': ['преглед', 'въведение', 'резюме', 'за нас', 'общ', 'основи', 'първи стъпки'],
        'mk': ['преглед', 'вовед', 'резиме', 'за нас', 'општ', 'основи', 'први чекори'],
        'sr-cyr': ['преглед', 'увод', 'резиме', 'о нама', 'опште', 'основе', 'први кораци'],
        'kk': ['шолу', 'кіріспе', 'қорытынды', 'біз туралы', 'жалпы', 'негіздер', 'бастау'],
        
        # Hindi and other Indic languages
        'hi': ['अवलोकन', 'परिचय', 'सारांश', 'के बारे में', 'सामान्य', 'मूल बातें', 'शुरुआत'],
        'mr': ['विहंगावलोकन', 'परिचय', 'सारांश', 'बद्दल', 'सामान्य', 'मूलभूत', 'सुरुवात'],
        'ne': ['अवलोकन', 'परिचय', 'सारांश', 'बारेमा', 'सामान्य', 'आधारभूत', 'सुरुवात'],
        'bn': ['সংক্ষিপ্ত বিবরণ', 'ভূমিকা', 'সারসংক্ষেপ', 'সম্পর্কে', 'সাধারণ', 'মৌলিক', 'শুরু করা'],
        'pa': ['ਸੰਖੇਪ', 'ਜਾਣ-ਪਛਾਣ', 'ਸਾਰ', 'ਬਾਰੇ', 'ਆਮ', 'ਬੁਨਿਆਦੀ', 'ਸ਼ੁਰੂਆਤ'],
        'gu': ['ઝાંખી', 'પરિચય', 'સારાંશ', 'વિશે', 'સામાન્ય', 'મૂળભૂત', 'શરૂઆત'],
        'si': ['දළ විශ්ලේෂණය', 'හැඳින්වීම', 'සාරාංශය', 'පිළිබඳ', 'සාමාන්‍ය', 'මූලික', 'ආරම්භය'],
        'ta': ['கண்ணோட்டம்', 'அறிமுகம்', 'சுருக்கம்', 'பற்றி', 'பொது', 'அடிப்படை', 'தொடக்கம்'],
        'te': ['అవలోకనం', 'పరిచయం', 'సారాంశం', 'గురించి', 'సాధారణ', 'ప్రాథమిక', 'ప్రారంభం'],
        'kn': ['ಅವಲೋಕನ', 'ಪರಿಚಯ', 'ಸಾರಾಂಶ', 'ಬಗ್ಗೆ', 'ಸಾಮಾನ್ಯ', 'ಮೂಲಭೂತ', 'ಪ್ರಾರಂಭ'],
        'ml': ['അവലോകനം', 'ആമുഖം', 'സംഗ്രഹം', 'കുറിച്ച്', 'പൊതു', 'അടിസ്ഥാന', 'ആരംഭം'],
        
        # Middle Eastern languages
        'ar': ['نظرة عامة', 'مقدمة', 'ملخص', 'حول', 'عام', 'أساسيات', 'البداية'],
        'fa': ['نمای کلی', 'مقدمه', 'خلاصه', 'درباره', 'عمومی', 'مبانی', 'شروع'],
        'ur': ['جائزہ', 'تعارف', 'خلاصہ', 'کے بارے میں', 'عمومی', 'بنیادی', 'شروعات'],
        'ps': ['عمومي کتنه', 'پیژندنه', 'لنډیز', 'د دې په اړه', 'عمومي', 'بنسټیز', 'پیل'],
        'he': ['סקירה', 'הקדמה', 'תקציר', 'אודות', 'כללי', 'יסודות', 'התחלה'],
        
        # East Asian languages
        'zh': ['概述', '介绍', '总结', '关于', '常规', '基础知识', '入门'],
        'zh-tw': ['概述', '介紹', '總結', '關於', '常規', '基礎知識', '入門'],
        'ja': ['概要', '紹介', '要約', 'について', '一般', '基本', '始める'],
        'ko': ['개요', '소개', '요약', '정보', '일반', '기초', '시작하기'],
        
        # Thai
        'th': ['ภาพรวม', 'แนะนำ', 'สรุป', 'เกี่ยวกับ', 'ทั่วไป', 'พื้นฐาน', 'เริ่มต้น'],
        
        # Greek
        'el': ['επισκόπηση', 'εισαγωγή', 'περίληψη', 'σχετικά με', 'γενικά', 'βασικά', 'ξεκίνημα'],
        
        # Indonesian and Malay
        'id': ['ikhtisar', 'pengantar', 'ringkasan', 'tentang', 'umum', 'dasar', 'memulai'],
        'ms': ['gambaran keseluruhan', 'pengenalan', 'ringkasan', 'mengenai', 'am', 'asas', 'bermula'],
        
        # Vietnamese
        'vi': ['tổng quan', 'giới thiệu', 'tóm tắt', 'về', 'chung', 'cơ bản', 'bắt đầu'],
        
        # African languages
        'sw': ['muhtasari', 'utangulizi', 'muhtasari', 'kuhusu', 'jumla', 'misingi', 'kuanza'],
        'ha': ['bayyani', 'gabatarwa', 'taƙaitaccen bayani', 'game da', 'gabaɗaya', 'tushe', 'farawa'],
        'ig': ['nlegharịta', 'mmeghe', 'nchịkọta', 'maka', 'izugbe', 'ntọala', 'mmalite']
    }

    CONTACT_TERMS = {
        # Default/English
        'en': ['contact', 'support', 'help', 'customer service', 'get in touch', 'reach us', 'assistance'],
        
        # Latin script European languages
        'de': ['kontakt', 'unterstützung', 'hilfe', 'kundendienst', 'kontaktieren', 'erreichen', 'unterstützung'],
        'es': ['contacto', 'soporte', 'ayuda', 'servicio al cliente', 'ponerse en contacto', 'comunicarse', 'asistencia'],
        'fr': ['contact', 'support', 'aide', 'service client', 'nous contacter', 'nous joindre', 'assistance'],
        'it': ['contatto', 'supporto', 'aiuto', 'servizio clienti', 'contattaci', 'raggiungerci', 'assistenza'],
        'pt': ['contato', 'suporte', 'ajuda', 'atendimento ao cliente', 'entre em contato', 'nos alcance', 'assistência'],
        'nl': ['contact', 'ondersteuning', 'hulp', 'klantenservice', 'contact opnemen', 'bereiken', 'assistentie'],
        'pl': ['kontakt', 'wsparcie', 'pomoc', 'obsługa klienta', 'skontaktuj się', 'dotrzeć', 'pomoc'],
        'ro': ['contact', 'suport', 'ajutor', 'serviciu clienți', 'luați legătura', 'ajungeți la noi', 'asistență'],
        'sv': ['kontakt', 'support', 'hjälp', 'kundservice', 'kontakta oss', 'nå oss', 'assistans'],
        'no': ['kontakt', 'støtte', 'hjelp', 'kundeservice', 'kontakt oss', 'nå oss', 'assistanse'],
        'da': ['kontakt', 'support', 'hjælp', 'kundeservice', 'kontakt os', 'nå os', 'assistance'],
        'fi': ['yhteystiedot', 'tuki', 'apu', 'asiakaspalvelu', 'ota yhteyttä', 'tavoita meidät', 'apu'],
        'hu': ['kapcsolat', 'támogatás', 'segítség', 'ügyfélszolgálat', 'lépjen kapcsolatba', 'elér minket', 'segítség'],
        'cs': ['kontakt', 'podpora', 'pomoc', 'zákaznický servis', 'kontaktujte nás', 'oslovte nás', 'asistence'],
        'sk': ['kontakt', 'podpora', 'pomoc', 'zákaznícky servis', 'kontaktujte nás', 'oslovte nás', 'asistencia'],
        'sl': ['stik', 'podpora', 'pomoč', 'služba za stranke', 'stopite v stik', 'dosegljivi smo', 'pomoč'],
        'hr': ['kontakt', 'podrška', 'pomoć', 'korisnička služba', 'kontaktirajte nas', 'dođite do nas', 'pomoć'],
        'bs': ['kontakt', 'podrška', 'pomoć', 'korisnička služba', 'kontaktirajte nas', 'dođite do nas', 'pomoć'],
        'tr': ['iletişim', 'destek', 'yardım', 'müşteri hizmetleri', 'iletişime geçin', 'bize ulaşın', 'yardım'],
        
        # Cyrillic script languages
        'ru': ['контакт', 'поддержка', 'помощь', 'служба поддержки', 'связаться с нами', 'обратиться', 'помощь'],
        'uk': ['контакт', 'підтримка', 'допомога', 'служба підтримки', "зв'язатися з нами", 'звернутися', 'допомога'],
        'bg': ['контакт', 'поддръжка', 'помощ', 'обслужване на клиенти', 'свържете се с нас', 'достигнете до нас', 'помощ'],
        'mk': ['контакт', 'поддршка', 'помош', 'услуга за клиенти', 'контактирајте не', 'допрете до нас', 'помош'],
        'sr-cyr': ['контакт', 'подршка', 'помоћ', 'корисничка служба', 'контактирајте нас', 'дођите до нас', 'помоћ'],
        'kk': ['байланыс', 'қолдау', 'көмек', 'тұтынушыларға қызмет', 'бізбен байланысыңыз', 'бізге жетіңіз', 'көмек'],
        
        # Hindi and other Indic languages
        'hi': ['संपर्क', 'सहायता', 'मदद', 'ग्राहक सेवा', 'संपर्क करें', 'हमसे मिलें', 'सहायता'],
        'mr': ['संपर्क', 'समर्थन', 'मदत', 'ग्राहक सेवा', 'संपर्क साधा', 'आमच्याशी संपर्क', 'सहाय्य'],
        'ne': ['सम्पर्क', 'समर्थन', 'सहायता', 'ग्राहक सेवा', 'सम्पर्क गर्नुहोस्', 'हामीलाई पुग्नुहोस्', 'सहायता'],
        'bn': ['যোগাযোগ', 'সাহায্য', 'সহায়তা', 'গ্রাহক সেবা', 'যোগাযোগ করুন', 'আমাদের কাছে পৌঁছান', 'সহায়তা'],
        'pa': ['ਸੰਪਰਕ', 'ਸਹਾਇਤਾ', 'ਮਦਦ', 'ਗਾਹਕ ਸੇਵਾ', 'ਸੰਪਰਕ ਕਰੋ', 'ਸਾਡੇ ਤੱਕ ਪਹੁੰਚੋ', 'ਸਹਾਇਤਾ'],
        'gu': ['સંપર્ક', 'આધાર', 'મદદ', 'ગ્રાહક સેવા', 'સંપર્ક કરો', 'અમારા સુધી પહોંચો', 'સહાય'],
        'si': ['සම්බන්ධතා', 'සහාය', 'උදව්', 'පාරිභෝගික සේවා', 'අපව සම්බන්ධ කරන්න', 'අප වෙත ළඟා වන්න', 'සහාය'],
        'ta': ['தொடர்பு', 'ஆதரவு', 'உதவி', 'வாடிக்கையாளர் சேவை', 'தொடர்பு கொள்ளுங்கள்', 'எங்களை அணுகுங்கள்', 'உதவி'],
        'te': ['సంప్రదింపులు', 'మద్దతు', 'సహాయం', 'కస్టమర్ సేవ', 'సంప్రదించండి', 'మాను చేరుకోండి', 'సహాయం'],
        'kn': ['ಸಂಪರ್ಕ', 'ಬೆಂಬಲ', 'ಸಹಾಯ', 'ಗ್ರಾಹಕ ಸೇವೆ', 'ಸಂಪರ್ಕಿಸಿ', 'ನಮ್ಮನ್ನು ತಲುಪಿ', 'ಸಹಾಯ'],
        'ml': ['ബന്ധപ്പെടുക', 'പിന്തുണ', 'സഹായം', 'ഉപഭോക്തൃ സേവനം', 'ബന്ധപ്പെടുക', 'ഞങ്ങളെ എത്തിക്കുക', 'സഹായം'],
        
        # Middle Eastern languages
        'ar': ['اتصال', 'دعم', 'مساعدة', 'خدمة العملاء', 'تواصل معنا', 'الوصول إلينا', 'مساعدة'],
        'fa': ['تماس', 'پشتیبانی', 'کمک', 'خدمات مشتری', 'تماس با ما', 'به ما برسید', 'کمک'],
        'ur': ['رابطہ', 'سپورٹ', 'مدد', 'کسٹمر سروس', 'رابطہ کریں', 'ہم تک پہنچیں', 'مدد'],
        'ps': ['اړیکه', 'ملاتړ', 'مرسته', 'د پیرودونکو خدماتو', 'موږ سره اړیکه ونیسئ', 'موږ ته ورسیږئ', 'مرسته'],
        'he': ['יצירת קשר', 'תמיכה', 'עזרה', 'שירות לקוחות', 'צור קשר', 'הגע אלינו', 'סיוע'],
        
        # East Asian languages
        'zh': ['联系', '支持', '帮助', '客户服务', '联系我们', '联系我们', '协助'],
        'zh-tw': ['聯絡', '支援', '幫助', '客戶服務', '聯絡我們', '聯絡我們', '協助'],
        'ja': ['連絡先', 'サポート', 'ヘルプ', 'カスタマーサービス', 'お問い合わせ', 'ご連絡', 'サポート'],
        'ko': ['연락처', '지원', '도움말', '고객 서비스', '문의하기', '연락하기', '지원'],
        
        # Thai
        'th': ['ติดต่อ', 'สนับสนุน', 'ช่วยเหลือ', 'บริการลูกค้า', 'ติดต่อเรา', 'เข้าถึงเรา', 'ความช่วยเหลือ'],
        
        # Greek
        'el': ['επικοινωνία', 'υποστήριξη', 'βοήθεια', 'εξυπηρέτηση πελατών', 'επικοινωνήστε μαζί μας', 'φτάστε μας', 'βοήθεια'],
        
        # Indonesian and Malay
        'id': ['kontak', 'dukungan', 'bantuan', 'layanan pelanggan', 'hubungi kami', 'jangkau kami', 'bantuan'],
        'ms': ['hubungi', 'sokongan', 'bantuan', 'perkhidmatan pelanggan', 'hubungi kami', 'capai kami', 'bantuan'],
        
        # Vietnamese
        'vi': ['liên hệ', 'hỗ trợ', 'giúp đỡ', 'dịch vụ khách hàng', 'liên hệ với chúng tôi', 'tiếp cận chúng tôi', 'hỗ trợ'],
        
        # African languages
        'sw': ['mawasiliano', 'msaada', 'msaada', 'huduma za wateja', 'wasiliana nasi', 'tufikieni', 'msaada'],
        'ha': ['sadarwa', 'tallafi', 'taimako', 'sabis na abokan ciniki', 'sadarwa da mu', 'kai gare mu', 'taimako'],
        'ig': ['kpọtụrụ', 'nkwado', 'enyemaka', 'ọrụ ndị ahịa', 'kpọtụrụ anyị', 'rute anyị', 'enyemaka']
    }