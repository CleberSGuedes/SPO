PTA 2026 - Sistema de Planejamento do Trabalho Anual

Este sistema tem como objetivo gerenciar o planejamento, execução e acompanhamento das ações educacionais da Secretaria de Estado de Educação de Mato Grosso (SEDUC-MT), conforme diretrizes estabelecidas para o Plano de Trabalho Anual (PTA) do ano de 2026.
🚀 Funcionalidades

    Cadastro de Programas, Ações (PAOE) e Produtos da Ação

    Gerenciamento de Subações/Entregas e Municípios vinculados

    Planejamento de Etapas por Subação

    Painel de Acompanhamento com dados em tempo real

    Validação automática com base nas regras institucionais

    Controle de sessões ativas por usuário

    Interface amigável com feedback visual (SweetAlert2)

🛠️ Tecnologias Utilizadas

    Python 3.10+

    Flask

    HTML5, CSS3 e Bootstrap 5

    JavaScript (modularizado)

    MySQL

    SQLAlchemy

    Jinja2

    SweetAlert2

📁 Estrutura do Projeto

cleber-pta/
├── app.py → Arquivo principal da aplicação Flask
├── templates/ → Arquivos HTML com Jinja2
├── static/
│ ├── css/ → Estilos personalizados
│ ├── js/ → Scripts JavaScript modulares
│ └── img/ → Imagens e logos
├── README.md → Documentação do projeto
└── requirements.txt → Dependências Python
🧪 Como Executar Localmente

    Clone o repositório:
    git clone https://github.com/CleberSGuedes/cleber-pta.git
    cd cleber-pta

    Crie e ative um ambiente virtual:
    python -m venv venv
    venv\Scripts\activate (Windows) ou source venv/bin/activate (Linux/macOS)

    Instale as dependências:
    pip install -r requirements.txt

    Execute a aplicação:
    python app.py

    Acesse no navegador:
    http://localhost:5000

🔐 Acesso e Segurança

    Configure variáveis sensíveis como credenciais do banco via .env (não versionado)

    Pode ser publicado via IIS para acesso interno institucional

📌 Status do Projeto

✅ Em desenvolvimento contínuo. Já utilizado internamente pela equipe da SEDUC-MT para acompanhamento do planejamento educacional.
🤝 Contribuição

Contribuições são bem-vindas!

    Faça um fork do projeto

    Crie uma branch com sua feature:
    git checkout -b minha-feature

    Commit suas alterações:
    git commit -m "feat: adiciona nova funcionalidade"

    Push para o repositório:
    git push origin minha-feature

    Abra um Pull Request

📄 Licença

Este projeto está licenciado sob a MIT License.
