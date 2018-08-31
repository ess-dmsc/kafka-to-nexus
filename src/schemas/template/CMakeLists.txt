set(template_module_SRC
)

set(template_module_INC
  TemplateWriter.h
)

create_module(template_module)

target_include_directories(template_module PRIVATE "../../")
