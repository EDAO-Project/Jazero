from django.shortcuts import render
from django.http import HttpResponse
from django.template import loader
from django.core.exceptions import BadRequest

import os
from cdlc.cdlc import Connector

def index(request):
    template = loader.get_template('cdlc/index.html')
    context = {}
    return HttpResponse(template.render(context, request))

# Make here an enpoint to handle POST request sent by the 'index' endpoint above for searching
def result(request):
    if request.method != 'POST':
        raise BadRequest("Request must be POST")

    host = os.environ['JAZERO_HOST']
    use_embeddings = 'Embeddings' in request.POST.getlist('settings')
    weighted_jaccard = 'Weighted_jaccard' in request.POST.getlist('settings')
    cosine_function = request.POST.getlist('settings')[-1]
    query = request.POST.getlist('query')[-1]

    if len(query) == 0:
        raise BadRequest("Missing query")

    # TODO: Finish getting results using connector
    template = loader.get_template('cdlc/index.html')
    context = {}
    conn = Connector(host)

    if not conn.isConnected():
        context = {
            'error': 'Could not connect to Jazero instance. Make sure all Jazero services are running.'
        }

    else:
        context = {
            'k': '100',
            'result': ['T1']
        }

    return HttpResponse(template.render(context, request))