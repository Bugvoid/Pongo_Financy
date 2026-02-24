## robo-bra-cra-fnet

- **./rnf/robo-bra-cra-fnet/download.py**: download de arquivos xml com informes CRA do FNET    
    - Usa tipo de certificado = CRA, 
          categoria = informes periódicos, 
          tipo = informe mensal de CRA (Anexo 32, II ICVM 480)        
    - **fonte:**
        https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCertificadosCVM#
    - **Link XML:**
        https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento?id=<id>&cvm=true

- **./rnf/robo-bra-cra-fnet/terraform/stage/main.tf**: Infraestrutura no aws backoffice para os informes CRA.
  - Na pasta .secret, ter as credenciais do backoffice para procedimento do deploy no aws, variáveis devem ser conferidas nos arquvios de cada pasta do terraform
  - executar /terraform/stage/tf.bat (nome do workspace: dev/teste/oficial) (init/plan/apply) ou outro comando terraform. ex.: tf.bat dev apply
  - O script faz a criação dos buckets, sns, sqs e suas policys de acordo com o workspace
