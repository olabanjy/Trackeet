from django.shortcuts import render

# Create your views here.

def homepage(request):
    
    template = 'home/welcome.html'

    return render(request, template)