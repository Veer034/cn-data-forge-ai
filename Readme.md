About

    DataForge AI symbolizes an AI system that processes, structures, and refines data to generate intelligent responses. It suggests a powerful, industrial-grade AI that forges knowledge from data, making it great for vector search, document processing, and AI-driven insights.

Yes! Your setup should have two models:

1️⃣ **For Sentence Embeddings & FAQ Matching (Vector Storage in Elasticsearch)**  
 ✅ **Model:** `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`  
 ✅ **Why?** Multilingual, small (~230MB), fast on CPU  
 ✅ **Use Case:** Convert documents, FAQs, and queries into vector embeddings before storing in Elasticsearch

2️⃣ **For Tokenization & Preprocessing (Multi-language BERT Model)**  
 ✅ **Model:** `bert-base-multilingual-cased`  
 ✅ **Why?** Supports 104 languages, preserves casing, good for tokenization  
 ✅ **Use Case:** Tokenize text before embedding, preprocessing for NLP tasks

---

## **🚀 How They Work Together**

🔹 **Step 1:** Tokenize text with `bert-base-multilingual-cased`  
🔹 **Step 2:** Convert text into **vectors** with `paraphrase-multilingual-MiniLM-L12-v2`  
🔹 **Step 3:** Store vectors in **Elasticsearch**  
🔹 **Step 4:** At query time, **search nearest vectors** for matching responses

---

## **⚡ Code Example**

### **1️⃣ Tokenization with Multilingual BERT**

```python
from transformers import AutoTokenizer

tokenizer = AutoTokenizer.from_pretrained("bert-base-multilingual-cased")

text = "Bonjour, comment puis-je vous aider?"
tokens = tokenizer.tokenize(text)

print(tokens)  # ['Bonjour', ',', 'comment', 'puis', '-', 'je', 'vous', 'aider', '?']
```

---

### **2️⃣ Convert Sentences to Vectors (Embeddings)**

```python
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

sentence = "Bonjour, comment puis-je vous aider?"
embedding = model.encode(sentence)

print(embedding.shape)  # (384,) → 384-dimensional vector
```

---

Yes, the models I mentioned (`paraphrase-multilingual-mpnet-base-v2`, `LaBSE`, and `XLM-RoBERTa`-based models) are all open source and can be downloaded for local use in production without licensing fees. They're available through the Hugging Face model hub and the sentence-transformers library.

In comparison to `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`:

1. **Performance advantages:**

   - `paraphrase-multilingual-mpnet-base-v2` offers better context understanding and generally higher accuracy for semantic similarity tasks across languages
   - `LaBSE` was specifically designed for cross-lingual alignment and performs particularly well when matching content across different languages
   - Both typically outperform MiniLM in semantic search tasks, especially for longer or more complex passages

2. **Trade-offs:**
   - These higher-performing models are generally larger than MiniLM-L12-v2
   - They require more computational resources and may be slightly slower for encoding
   - `mpnet-base-v2` is about 420MB vs MiniLM-L12-v2's 120MB

You can use them with essentially the same code you already have, just changing the model path:

```python
from sentence_transformers import SentenceTransformer

# Using mpnet instead of MiniLM
model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')
```

For production use, I'd recommend downloading the model files during your container build or setup process, rather than downloading at runtime. This ensures your application has the model available even if the Hugging Face servers are unavailable.

### **3️⃣ Store in Elasticsearch (FAISS or HNSW)**

You can now **store embeddings in Elasticsearch** and **search for similar queries**.

Would you like help setting up **vector search in Elasticsearch** for this? 🚀

Remove docker images

    docker-compose down

Just for building

    docker-compose build

Build with new code

    docker-compose up --build

Remove old venv

    rm -rf venv

Install Python

    brew install python@3.10

Activiate Env

    #can change the env names
    python3.10 -m venv myvenv
    source myvenv/bin/activate

    #make sure version is 3.10.*
    python --version

Install library in local VM

    #Required for kafka confluent
    brew install librdkafka

    pip install "numpy<2.0.0" aiohttp sentence-transformers elasticsearch confluent-kafka httpx nltk python-dotenv transformers langdetect pydantic

    #For language detection
    pip install langdetect fasttext lingua-language-detector pycld2 polyglot pyicu morfessor

Start in local

    python3.10 master.py

deactivate your virtual environment if it's active:

    deactivate

# Production Setup

### Login VM

    ssh azureuser@YOUR-VM-PUBLIC-IP

### Install Git

    sudo apt update
    sudo apt install git -y
    git clone https://github.com/Veer034/cn-data-forge-ai.git

### Install Python

    sudo add-apt-repository ppa:deadsnakes/ppa -y
    sudo apt update
    sudo apt install python3.10 python3.10-venv python3.10-distutils

### Install library in production VM

    pip install "numpy<2.0.0" aiohttp sentence-transformers elasticsearch confluent-kafka httpx nltk python-dotenv transformers langdetect pydantic

    # Install all required build tools and dependencies for language libraries
    sudo apt update
    sudo apt install -y \
        build-essential \
        g++ \
        gcc \
        python3-dev \
        libicu-dev \
        pkg-config \
        cmake \
        make \
        git \
        libc6-dev \
        linux-headers-generic

    # Install additional tools that FastText needs
    sudo apt install -y \
        software-properties-common \
        apt-transport-https \
        ca-certificates \
        gnupg \
        lsb-release

    # Check current GCC version
    gcc --version

    # If GCC is older than 7.x, update it
    sudo apt install -y gcc-9 g++-9
    sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 60
    sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-9 60

    # Ensure pip build tools are up to date
    pip install --upgrade pip setuptools wheel
    pip install --upgrade build

    #For language detection
    pip install langdetect fasttext lingua-language-detector pycld2 polyglot pyicu morfessor

### Create Systemd file for as a service execution

    sudo tee /etc/systemd/system/cn-data-forge-ai.service > /dev/null << EOF
    [Unit]
    Description=For data forging
    After=network.target ollama.service
    Requires=ollama.service

    [Service]
    Type=simple
    User=azureuser
    WorkingDirectory=/home/azureuser/cn-data-forge-ai
    Environment=PATH=/home/azureuser/cn-data-forge-ai/myvenv/bin
    ExecStart=/home/azureuser/cn-data-forge-ai/myvenv/bin/python master.py
    Restart=always
    RestartSec=10
    StandardOutput=journal
    StandardError=journal

    [Install]
    WantedBy=multi-user.target
    EOF

### HuggingFace model storage location

    ~/.cache/huggingface/

### List all services

    systemctl list-units --type=service
    systemctl list-units --type=service | grep cn-

### Reload systemd

    sudo systemctl daemon-reload

### Enable all services to start on boot

    sudo systemctl enable cn-data-forge-ai

### Start Service

    sudo systemctl start cn-data-forge-ai

### Check Status

    sudo systemctl status cn-data-forge-ai

### Stop service

    sudo systemctl stop cn-data-forge-ai

### Restart service

    sudo systemctl restart cn-data-forge-ai

### Check logs for specific service

    sudo journalctl -u cn-data-forge-ai -f

### Check service generated logs

    tail -n 50 ~/cn-data-forge-ai/logs/server.log

### List all topics

kafka-topics.sh --list --bootstrap-server 57.159.53.43:9092

### Create a specific topic

kafka-topics.sh --create --topic my-topic --bootstrap-server 57.159.53.43:9092

### Describe a specific topic

kafka-topics.sh --describe --topic tenant.documents.vector.storage.request --bootstrap-server 57.159.53.43:9092

### Describe all topics

kafka-topics.sh --describe --bootstrap-server 57.159.53.43:9092

### Describe multiple specific topics

kafka-topics.sh --describe --topic tenant.documents.vector.storage.request,tenant.faq.vector.storage.request --bootstrap-server 57.159.53.43:9092

Kafka input formt:

    {
    "tenant_id": "tenant123",
    "content": "# Company Policy Document\n\n## Privacy Policy\n\nWe value your privacy and are committed to protecting your personal information. This policy explains how we collect and use your data.\n\n## Frequently Asked Questions\n\nQ: How is my data stored?\nA: All data is encrypted and stored in secure cloud servers with restricted access.\n\nQ: Can I request deletion of my information?\nA: Yes. You can request complete deletion of your personal data by contacting our support team.\n\n## Data Processing Guidelines\n\nPersonal information is only processed for the purposes explicitly stated during collection. Our team follows strict access protocols when handling customer data.\n\nIf you have questions about data processing, please contact our data protection officer.",
    "metadata": {
        "timestamp": "2025-02-24T14:32:45.123Z",
        "document_type": "policy",
        "language": "en"
        }
    }

Kafka publsih command:

    English tenant A

# Create a temporary file

cat > temp_message.json << 'EOF'
{
"tenantId": "tenantA",
"documentId": "f47f390f-fa39-49cd-b66e-328823a0044a",
"content": "1. Company History\n\n [Company Name] was founded in [Year] with a vision to revolutionize the way people shop online. What started as a small marketplace with a few carefully curated products has now grown into a leading global e-commerce platform serving millions of customers.\n\nFrom the very beginning, our goal was to provide customers with high-quality products at competitive prices while ensuring a seamless shopping experience. We focused on building strong relationships with reliable suppliers, manufacturers, and logistics partners to offer a diverse range of products across various categories, including electronics, fashion, home essentials, beauty, and more.\n\nOver the years, we have continuously innovated, integrating the latest technology to enhance customer experience. From AI-driven recommendations to secure payment gateways and real-time order tracking, we strive to make online shopping convenient, secure, and enjoyable.\n\nCustomer satisfaction remains at the heart of our business. Our dedicated customer support team is available 24/7 to assist shoppers with inquiries, returns, and order tracking. As we expand globally, we remain committed to sustainability, ethical sourcing, and offering a personalized shopping experience for all our customers.\n\nToday, [Company Name] is more than just an online store—it's a trusted shopping destination where quality, affordability, and convenience come together.\n\n---\n\n2. Terms & Conditions\n\nWelcome to [Company Name]. By accessing and using our website, you agree to the following terms and conditions. Please read them carefully.\n\n1. User Agreement\nBy using our platform, you confirm that you are at least 18 years old or have parental consent to use the service. You are responsible for maintaining the confidentiality of your account credentials and agree not to share your login details with others.\n\n2. Orders & Payments\nAll orders placed through our website are subject to availability and confirmation. We accept multiple payment methods, including credit/debit cards, PayPal, and digital wallets. Prices listed on our website may change without prior notice.\n\n3. Shipping & Delivery\nWe offer standard and expedited shipping options. Estimated delivery times are provided at checkout, but delays may occur due to unforeseen circumstances. International customers are responsible for any customs duties or taxes applicable in their country.\n\n4. Returns & Refunds\nOur return policy allows customers to return eligible items within 30 days of delivery. Items must be unused and in their original packaging. Refunds are processed within 7-10 business days after the returned item is received and inspected.\n\n5. Limitation of Liability\nWe strive to provide accurate product descriptions, but we do not guarantee that product details, images, or pricing are error-free. We are not liable for any indirect or consequential damages arising from product use.\n\n6. Changes to Terms\nWe reserve the right to update these terms at any time. Continued use of our website implies acceptance of any revised terms.\n\nFor any questions, please contact [Customer Support Email].\n\n---\n\n3. Return & Refund Policy\n\nAt [Company Name], we want you to be completely satisfied with your purchase. If you're not happy with your order, we offer a hassle-free return and refund policy.\n\nEligibility for Returns\n- Items must be returned within 30 days from the date of delivery.\n- Products should be unused, undamaged, and in their original packaging with all tags and labels intact.\n- Certain items, such as personal care products, customized goods, and perishable items, are non-returnable for hygiene and safety reasons.\n\nHow to Initiate a Return\n1. Log in to your account and navigate to the Orders section.\n2. Select the item you want to return and submit a return request.\n3. Pack the item securely and use the provided return label for shipping.\n4. Once we receive the returned item, it will be inspected within 3-5 business days.\n\nRefund Process\n- Refunds will be issued via the original payment method within 7-10 business days after return approval.\n- If you received a defective or incorrect product, we will cover the return shipping costs.\n- Refunds for items purchased during sales or promotions will be based on the discounted price paid.\n\nFor further assistance, contact our Customer Support Team at [Support Email] or via live chat.\n\n---\n\n4. General Information\n\nCompany Overview\n[Company Name] is a trusted e-commerce platform that offers a wide variety of high-quality products, from electronics and fashion to home essentials and beauty products. Our mission is to provide a seamless shopping experience with affordable prices, fast shipping, and excellent customer service.\n\nCustomer Support\nWe believe in putting customers first. Our 24/7 customer support team is available via:\n- Live Chat (on our website)\n- Email: [Support Email]\n- Phone: [Support Number]\n\nAccepted Payment Methods\nWe accept multiple payment options to make shopping convenient:\n- Credit/Debit Cards (Visa, Mastercard, Amex, Discover)\n- PayPal & Apple Pay\n- Bank Transfers & Digital Wallets\n\nShipping Information\nWe ship to over 50 countries worldwide. Our shipping options include:\n- Standard Shipping (5-7 business days)\n- Express Shipping (2-3 business days)\n- Same-Day Delivery (in select cities)\n\nSecurity & Privacy\nYour data is protected with SSL encryption and strict privacy policies to ensure a safe shopping experience. We do not share or sell customer information to third parties.\n\nThank you for choosing [Company Name] for your shopping needs! 🚀",
"metadata": {
"timestamp": "2025-02-24T14:32:45.123Z"
}
}
EOF

# Use the file to publish to Kafka

cat temp_message.json | jq -c . | docker exec -i broker kafka-console-producer \
 --bootstrap-server localhost:9092 \
 --topic tenant.documents.vector.storage.request \
 --property "parse.key=false" \
 --property "key.separator=,"

# Remove the temporary file

rm temp_message.json

    English tenant B

    # Create a temporary file for the Travel company document

cat > travel_company.json << 'EOF'
{
"tenantId": "tenantB",
"documentId": "f47f390f-fa39-49cd-b66e-328823a0044b",
"content": "# **1. Company History** \n\n[Company Name] was founded in [Year] with the mission of revolutionizing travel and transportation services. What started as a small local transportation provider has now grown into a global company offering a wide range of travel solutions, including flight bookings, hotel reservations, car rentals, and ride-hailing services. \n\nOur goal is to make travel **affordable, accessible, and seamless** for everyone. Over the years, we have partnered with leading airlines, hotels, and transportation providers worldwide to offer the best travel experiences at competitive prices. \n\nTechnology is at the core of our operations. We leverage **AI-powered recommendations, real-time tracking, and automated booking systems** to enhance customer convenience. Whether you're planning a business trip, a family vacation, or a quick city commute, we ensure a **hassle-free travel experience** with 24/7 customer support and flexible booking options. \n\nToday, [Company Name] serves millions of travelers across multiple countries, helping them navigate their journeys with comfort and confidence. \n\n---\n\n# **2. Terms and Conditions** \n\nWelcome to [Company Name]. By using our services, you agree to the following terms and conditions. \n\n### **1. User Agreement** \n- Users must be **18 years or older** or have parental consent to use our services. \n- Account details must be accurate, and users are responsible for maintaining their security. \n\n### **2. Booking and Payment** \n- All bookings are subject to availability and confirmation. \n- Accepted payment methods: **Credit/Debit Cards (Visa, Mastercard, Amex), PayPal, and Digital Wallets**. \n- Prices and availability are subject to change without notice. \n\n### **3. Cancellations and Refunds** \n- Cancellation policies vary depending on the service provider (airlines, hotels, car rentals). \n- Refunds, if applicable, are processed within **7-10 business days** after cancellation. \n- Some bookings may be non-refundable or subject to cancellation fees. \n\n### **4. Travel Policies** \n- Users must comply with airline, hotel, and local travel regulations. \n- Visas and other travel documents are the responsibility of the traveler. \n- The company is not responsible for missed flights, visa denials, or other personal travel issues. \n\n### **5. Liability Limitations** \n- We are not responsible for travel disruptions caused by **weather, strikes, government regulations, or third-party service providers**. \n- Users are responsible for their personal belongings during travel. \n\n### **6. Changes to Terms** \n- We reserve the right to modify these terms at any time. Changes will be posted on our website. \n\nFor any concerns, please contact **[Customer Support Contact]**. \n\n---\n\n# **3. Refund & Cancellation Policy** \n\nAt [Company Name], we strive to provide a flexible and customer-friendly booking experience. Below is our cancellation and refund policy. \n\n### **1. Flight Cancellations** \n- Airline cancellation policies apply. Some tickets may be **non-refundable**. \n- Refunds, if applicable, will be processed in **7-14 business days**. \n\n### **2. Hotel Cancellations** \n- Cancellation policies vary by hotel. Some may allow free cancellations **up to 48 hours before check-in**, while others may charge a penalty. \n- Refunds are subject to the hotel's terms and will be credited to your original payment method. \n\n### **3. Car Rentals & Ride-Hailing** \n- Cancellations within **24 hours of pick-up** may be subject to fees. \n- Refunds for pre-paid rentals are processed within **5-7 business days**. \n\n### **4. Tour & Activity Bookings** \n- Some tour bookings may be **non-refundable**. Please check the cancellation terms before booking. \n\nIf you need assistance, contact our **24/7 customer support team**. \n\n---\n\n# **4. General Information** \n\n### **About Us** \n[Company Name] is a leading travel and transportation provider offering **flight bookings, hotel reservations, car rentals, ride-hailing, and guided tours** worldwide. Our goal is to simplify travel with reliable services and competitive pricing. \n\n### **Customer Support** \nWe offer **24/7 multilingual customer support** to assist with bookings, cancellations, and travel inquiries. \n- **Live Chat:** Available on our website \n- **Email:** [Support Email] \n- **Phone:** [Support Phone Number] \n\n### **Accepted Payment Methods** \n- **Credit/Debit Cards**: Visa, Mastercard, Amex \n- **Digital Wallets**: PayPal, Apple Pay, Google Pay \n- **Bank Transfers** (for large bookings) \n\n### **Travel Insurance** \nWe recommend purchasing **travel insurance** for added protection against trip cancellations, medical emergencies, and lost luggage. \n\n### **Security and Privacy** \nWe prioritize user privacy by implementing **SSL encryption** and do not share customer data with third parties. \n\nThank you for choosing [Company Name] for your travel needs! 🚀 ",
"metadata": {
"timestamp": "2025-02-24T15:32:45.123Z"
}
}
EOF

# Use the file to publish to Kafka

cat travel_company.json | jq -c . | docker exec -i broker kafka-console-producer \
 --bootstrap-server localhost:9092 \
 --topic tenant.documents.vector.storage.request \
 --property "parse.key=false" \
 --property "key.separator=,"

# Remove the temporary file

rm travel_company.json

    Spanish tenant C

# Create a temporary file for the Spanish document

cat > spanish_company.json << 'EOF'
{
"tenantId": "tenantA",
"documentId": "f47f390f-fa39-49cd-b66e-328823a0044c",
"content": "# **1. Historia de la Empresa** \n\n**[Nombre de la Empresa]** fue fundada en **[Año]** con la visión de revolucionar la forma en que las personas compran en línea. Lo que comenzó como un pequeño mercado con algunos productos cuidadosamente seleccionados, hoy se ha convertido en una de las plataformas de comercio electrónico líderes a nivel mundial, sirviendo a millones de clientes. \n\nDesde el principio, nuestro objetivo ha sido ofrecer productos de alta calidad a precios competitivos, garantizando una experiencia de compra fluida. Hemos trabajado para establecer relaciones sólidas con proveedores confiables, fabricantes y socios logísticos, lo que nos permite ofrecer una amplia variedad de productos en categorías como electrónicos, moda, hogar, belleza y más. \n\nA lo largo de los años, hemos innovado constantemente, incorporando las últimas tecnologías para mejorar la experiencia del cliente. Desde recomendaciones impulsadas por inteligencia artificial hasta pasarelas de pago seguras y seguimiento de pedidos en tiempo real, nos esforzamos por hacer que las compras en línea sean convenientes, seguras y satisfactorias. \n\nLa satisfacción del cliente es el núcleo de nuestro negocio. Nuestro equipo de atención al cliente está disponible **24/7** para ayudar con consultas, devoluciones y seguimiento de pedidos. A medida que nos expandimos globalmente, seguimos comprometidos con la sostenibilidad, la ética en la cadena de suministro y una experiencia de compra personalizada. \n\nHoy, **[Nombre de la Empresa]** es más que una tienda en línea: es un destino de compras de confianza donde la calidad, el precio y la comodidad se unen. \n\n---\n\n# **2. Términos y Condiciones** \n\nBienvenido a **[Nombre de la Empresa]**. Al acceder y utilizar nuestro sitio web, usted acepta los siguientes términos y condiciones. Por favor, léalos detenidamente. \n\n### **1. Acuerdo del Usuario** \nAl utilizar nuestra plataforma, usted confirma que tiene al menos **18 años** o cuenta con el consentimiento de sus padres o tutores legales. Usted es responsable de mantener la confidencialidad de sus credenciales de cuenta y se compromete a no compartir su información de acceso con terceros. \n\n### **2. Pedidos y Pagos** \nTodos los pedidos realizados a través de nuestro sitio web están sujetos a disponibilidad y confirmación. Aceptamos diversos métodos de pago, incluyendo tarjetas de crédito/débito, PayPal y billeteras digitales. Los precios pueden cambiar sin previo aviso. \n\n### **3. Envío y Entrega** \nOfrecemos opciones de envío estándar y exprés. Los tiempos estimados de entrega se proporcionan en la confirmación del pedido, pero pueden verse afectados por retrasos imprevistos. Los clientes internacionales son responsables de cualquier impuesto aduanero aplicable en su país. \n\n### **4. Devoluciones y Reembolsos** \nNuestra política de devoluciones permite a los clientes devolver artículos elegibles dentro de los **30 días** posteriores a la entrega. Los productos deben estar sin usar y en su empaque original. Los reembolsos se procesan en un plazo de **7-10 días hábiles** después de recibir y revisar la devolución. \n\n### **5. Limitación de Responsabilidad** \nNos esforzamos por proporcionar descripciones precisas de los productos, pero no garantizamos que los detalles, imágenes o precios sean completamente libres de errores. No somos responsables de daños indirectos derivados del uso de los productos. \n\n### **6. Cambios en los Términos** \nNos reservamos el derecho de actualizar estos términos en cualquier momento. El uso continuo de nuestro sitio web implica la aceptación de los términos revisados. \n\nPara consultas, por favor, contacte a **[Correo de Atención al Cliente]**. \n\n---\n\n# **3. Política de Devoluciones y Reembolsos** \n\nEn **[Nombre de la Empresa]**, queremos que esté completamente satisfecho con su compra. Si no está conforme con su pedido, ofrecemos una política de devoluciones sencilla y sin complicaciones. \n\n### **Elegibilidad para Devoluciones** \n- Los artículos deben devolverse dentro de los **30 días** posteriores a la entrega. \n- Los productos deben estar **sin usar, sin daños y en su empaque original** con todas las etiquetas intactas. \n- Algunos artículos, como **productos de cuidado personal, bienes personalizados y artículos perecederos**, no son retornables por razones de higiene y seguridad. \n\n### **Cómo Iniciar una Devolución** \n1. Inicie sesión en su cuenta y vaya a la sección **Pedidos**. \n2. Seleccione el artículo que desea devolver y envíe una solicitud de devolución. \n3. Empaque el artículo de manera segura y use la etiqueta de devolución proporcionada. \n4. Una vez que recibamos el artículo, será inspeccionado en un plazo de **3-5 días hábiles**. \n\n### **Proceso de Reembolso** \n- Los reembolsos se emiten a través del método de pago original en un plazo de **7-10 días hábiles** después de la aprobación de la devolución. \n- Si recibió un producto defectuoso o incorrecto, cubriremos los costos de envío de la devolución. \n- Los reembolsos de productos comprados en promociones o descuentos se basarán en el precio pagado. \n\nPara más ayuda, contacte a nuestro **Equipo de Atención al Cliente** a través de **[Correo de Soporte]** o chat en vivo. \n\n---\n\n# **4. Información General** \n\n### **Resumen de la Empresa** \n**[Nombre de la Empresa]** es una plataforma de comercio electrónico confiable que ofrece una amplia gama de productos de alta calidad, desde electrónicos y moda hasta artículos para el hogar y productos de belleza. Nuestra misión es brindar una experiencia de compra sin inconvenientes con precios accesibles, envíos rápidos y un excelente servicio al cliente. \n\n### **Atención al Cliente** \nNuestra prioridad es usted. Nuestro equipo de **atención al cliente 24/7** está disponible a través de: \n- **Chat en Vivo** (en nuestro sitio web) \n- **Correo Electrónico**: [Correo de Soporte] \n- **Teléfono**: [Número de Atención] \n\n### **Métodos de Pago Aceptados** \nOfrecemos diversas opciones de pago para su comodidad: \n- **Tarjetas de Crédito/Débito (Visa, Mastercard, Amex, Discover)** \n- **PayPal & Apple Pay** \n- **Transferencias Bancarias & Billeteras Digitales** \n\n### **Información de Envío** \nEnviamos a **más de 50 países en todo el mundo**. Opciones de envío disponibles: \n- **Envío Estándar** (5-7 días hábiles) \n- **Envío Exprés** (2-3 días hábiles) \n- **Entrega en el Mismo Día** (en ciudades seleccionadas) \n\n### **Seguridad & Privacidad** \nSu información está protegida mediante **cifrado SSL** y estrictas políticas de privacidad para garantizar una experiencia de compra segura. No compartimos ni vendemos información de clientes a terceros. \n\nGracias por elegir **[Nombre de la Empresa]** como su destino de compras en línea. 🚀 ",
"metadata": {
"timestamp": "2025-02-24T16:32:45.123Z"
}
}
EOF

# Use the file to publish to Kafka

cat spanish_company.json | jq -c . | docker exec -i broker kafka-console-producer \
 --bootstrap-server localhost:9092 \
 --topic tenant.documents.vector.storage.request \
 --property "parse.key=false" \
 --property "key.separator=,"

# Remove the temporary file

rm spanish_company.json

    Japanese tenant D

# Create a temporary file for the Japanese document

cat > japanese_company.json << 'EOF'
{
"tenantId": "tenantA",
"documentId": "f47f390f-fa39-49cd-b66e-328823a0044d",
"content": "# **1. 会社の歴史** \n\n**[会社名]** は **[設立年]** に創立され、オンラインショッピングのあり方を変革することを目指してスタートしました。最初は厳選された少数の商品を扱う小規模なマーケットでしたが、現在では世界中の何百万ものお客様にご利用いただいている主要な EC プラットフォームの 1 つとなっています。 \n\n 当社の目標は、**高品質な商品を手頃な価格で提供し、スムーズな購買体験を実現すること** です。そのために、信頼できるサプライヤー、メーカー、物流パートナーとの強固な関係を築き、**電子機器、ファッション、家庭用品、美容製品** など、幅広いカテゴリの商品を提供しています。 \n\n また、最新技術を導入し、AI を活用したおすすめ機能、安全な決済システム、リアルタイムの配送追跡など、より便利で安全なショッピング体験を提供しています。 \n\n お客様満足度は当社の最優先事項です。**24 時間 365 日対応のカスタマーサポート** を提供し、返品・交換や配送の問題にも迅速に対応します。 \n\n 現在、**[会社名]** は、単なるオンラインショップではなく、**品質・価格・利便性を兼ね備えた信頼できるショッピングプラットフォーム** へと成長しました。 \n\n--- \n\n# **2. 利用規約** \n\n**[会社名]** へようこそ。本サイトをご利用いただくにあたり、以下の利用規約に同意したものとみなされます。 \n\n### **1. ユーザー契約** \n- 当サイトの利用は **18 歳以上** の方、または保護者の同意を得た未成年者に限ります。 \n- アカウントの情報を安全に管理し、第三者と共有しないでください。 \n\n### **2. 注文と支払い** \n- すべての注文は在庫状況および当社の承認に基づきます。 \n- 利用可能な支払い方法: **クレジットカード（Visa, Mastercard, Amex）、PayPal、デジタルウォレット** など。 \n- 価格は予告なく変更される場合があります。 \n\n### **3. 配送と納期** \n- 通常配送と**速達配送** の 2 種類をご用意しています。 \n- 配送予定は注文確認時に通知されますが、遅延が発生する場合があります。 \n- 国際配送の場合、**関税・税金はお客様の負担** となります。 \n\n### **4. 返品・返金** \n- 返品可能期間は **商品到着後 30 日以内** です。 \n- 商品は **未使用・未開封の状態で返品** してください。 \n- 返金処理には **7〜10 営業日** かかる場合があります。 \n\n### **5. 責任の制限** \n- 商品の詳細や価格に誤りがある可能性がございます。 \n- 製品使用による間接的損害には責任を負いません。 \n\n### **6. 規約の変更** \n- 当社は、事前通知なしに利用規約を変更する権利を有します。 \n\n ご不明点がございましたら、**[カスタマーサポートの連絡先]** までお問い合わせください。 \n\n--- \n\n# **3. 返品・返金ポリシー** \n\n**[会社名]** では、お客様にご満足いただけるショッピング体験を提供するため、簡単な**返品・返金プロセス** を用意しています。 \n\n### **返品対象** \n- 商品到着後 **30 日以内** に返品可能です。 \n- **未使用・未開封** の状態で、元のパッケージに入っていることが必要です。 \n- **化粧品、衛生用品、オーダーメイド品、食品** など、一部の商品は返品不可です。 \n\n### **返品方法** \n1. **アカウントページ** にログインし、「注文履歴」から返品リクエストを送信。 \n2. 返品ラベルを使用して、商品を安全に梱包。 \n3. 当社が商品を受領後、**3〜5 営業日以内** に検品。 \n\n### **返金処理** \n- 返金は元のお支払い方法に対し、**7〜10 営業日以内** に処理されます。 \n- 初期不良・誤配送の場合、返品送料は当社が負担いたします。 \n- 割引価格で購入された商品は、支払い済みの金額に応じて返金されます。 \n\n 詳細については、**[カスタマーサポート]** までご連絡ください。 \n\n--- \n\n# **4. 一般情報** \n\n### **会社概要** \n**[会社名]** は、信頼性の高い EC プラットフォームで、**電子機器、ファッション、家庭用品、美容製品** など、幅広い商品を取り揃えています。お客様にとって最適な価格とサービスを提供することを目指しています。 \n\n### **カスタマーサポート** \n 当社の**24 時間 365 日対応のカスタマーサポート** へは、以下の方法でご連絡いただけます。 \n- **ライブチャット**（公式ウェブサイト内） \n- **メール**: [カスタマーサポートメールアドレス] \n- **電話**: [サポート電話番号] \n\n### **支払い方法** \n 以下のお支払い方法をご利用いただけます。 \n- **クレジットカード（Visa, Mastercard, Amex, Discover）** \n- **PayPal & Apple Pay** \n- **銀行振込 & デジタルウォレット** \n\n### **配送情報** \n 当社は **50 カ国以上** に発送対応しています。 \n- **標準配送**（5〜7 営業日） \n- **速達配送**（2〜3 営業日） \n- **即日配送**（対象地域のみ） \n\n### **セキュリティとプライバシー** \n お客様の情報を保護するため、**SSL 暗号化** を採用し、個人情報は厳格に管理しています。お客様のデータを第三者と共有または販売することはありません。 \n\n**[会社名]** をご利用いただきありがとうございます！🚀",
"metadata": {
"timestamp": "2025-02-24T17:32:45.123Z"
}
}
EOF

# Use the file to publish to Kafka

cat japanese_company.json | jq -c . | docker exec -i broker kafka-console-producer \
 --bootstrap-server localhost:9092 \
 --topic tenant.documents.vector.storage.request \
 --property "parse.key=false" \
 --property "key.separator=,"

# Remove the temporary file

rm japanese_company.json
