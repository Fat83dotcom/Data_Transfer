# Data_Transfer
##Script para transferir dados entre banco de dados PostgreSQL

* Este programa foi usado para transferir dados entre banco de dados diferentes.
Basicamente ele faz um `SELECT` em um banco `origin`, manipula os dados recebidos para criar outra query de inserção e os insere em outra tabela, em outro banco, `destiny`.
* Ele basicamente é um daemon, e foi criado assim para poder rodar em servidores em nuvem, onde geralmente as infraestruturas de rede são mais rápidas, otimizando a velocidade das operações.
* A complexidade dos algoritmos de criação das queries é O(n), a do executor é O(n²), com complexidade de espaço O(1), para cada operação.
* Ele pode ser adaptado para qualquer operação de transferência simples de `SELECT` e `INSERT`, ou qualquer outra que se adaptar.
* Para compilar o programa é requisito ter as bibliotecas instaladas:

- API PostgreSQL para C++ [PQXX](https://github.com/jtv/libpqxx)
- Formatador de strings C++ [fmt](https://github.com/fmtlib/fmt) - c++20

* Os arquivos CMakeLists.txt e FindPQXX.cmake já estão no repositório, ou podem ser encontrados [aqui](https://gist.github.com/Fat83dotcom/d67ef7b4c8ad948df11e637e416eeaa7)

* O programa foi compilado com Linux Ubuntu 22.04.4 LTS, gcc 11.4.0, CMake 3.22.1

* O script Python foi criado para o contexto do programa.
