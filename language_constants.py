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