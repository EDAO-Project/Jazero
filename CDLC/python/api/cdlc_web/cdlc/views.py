from django.shortcuts import render
from django.http import HttpResponse
from django.template import loader

def index(request):
    template = loader.get_template('cdlc/index.html')
    context = {
        'test_message': 'Hello, World!'
    }
    return HttpResponse(template.render(context, request))

# Make here an enpoint to handle POST request sent by the 'index' endpoint above for searching