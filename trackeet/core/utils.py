from io import BytesIO
from django.http import HttpResponse
from django.template.loader import get_template
from .models import *
from django.core.files.base import ContentFile
from django.core.files import File

from xhtml2pdf import pisa



# def new_render_to_file(template_src, filename, txn_code, context_dict={}):
#     template = get_template(template_src)
#     html  = template.render(context_dict)
#     result = BytesIO()
#     pdf = pisa.pisaDocument(BytesIO(html.encode("UTF-8")), result)
#     investment = UserInvestment.objects.get(txn_code=txn_code)
#     print(investment.amount)
#     if not pdf.err:
#         with open(filename, 'wb+') as output:
#             pdf = pisa.pisaDocument(BytesIO(html.encode("UTF-8")), output)
#             investment.contract_file.save(filename, output)
#         return HttpResponse(result.getvalue(), content_type='application/pdf')
#     return None 





def new_render_to_file_label(template_src, filename, label_id, context_dict={}):
    template = get_template(template_src)
    html  = template.render(context_dict)
    result = BytesIO()
    print("debug 1")
    pdf = pisa.pisaDocument(BytesIO(html.encode("UTF-8")), result)
    print("debug 2")
    label = ProcessedLabelFile.objects.get(pk=label_id)
    print("debug 3")
    if not pdf.err:
        with open(filename, 'wb+') as output:
            pdf = pisa.pisaDocument(BytesIO(html.encode("UTF-8")), output)
            label.file_doc.save(filename, output)
        return HttpResponse(result.getvalue(), content_type='application/pdf')
    return None 

