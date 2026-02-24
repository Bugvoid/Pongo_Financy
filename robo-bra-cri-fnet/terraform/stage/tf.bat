if '%1' == '' goto syntax
if '%2' == '' goto syntax

call set aws_profile=bo
call terraform workspace new %1
call terraform workspace select %1

call terraform %2 %3 %4 %5 %6 -var-file ../variables.tfvars -var-file ./variables.tfvars -var-file ../../.secret/provider.tfvars

goto fim

:syntax
@echo.
@echo ERRO: tf.bat (nome do workspace: dev/teste/oficial) (init/plan/apply) ou outro comando terraform
@echo ex.: tf.bat dev apply
@echo.

 :fim