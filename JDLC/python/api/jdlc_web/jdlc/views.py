from django.shortcuts import render
from django.http import HttpResponse
from django.template import loader
from django.core.exceptions import BadRequest

import os
from jdlc.jdlc import Connector
import json

def index(request):
    template = loader.get_template('jdlc/index.html')
    context = {}
    return HttpResponse(template.render(context, request))

# Make here an enpoint to handle POST request sent by the 'index' endpoint above for searching
# TODO: This is temporary and needs more options
def result(request):
    if request.method != 'POST':
        raise BadRequest("Request must be POST")

    host = os.environ['JAZERO_HOST']
    use_embeddings = 'Embeddings' in request.POST.getlist('settings')
    weighted_jaccard = 'Weighted_jaccard' in request.POST.getlist('settings')
    cosine_function = request.POST.getlist('settings')[-1]
    query = request.POST.getlist('query')[-1]
    scoring_type = None
    query_table = list()

    if len(query) == 0:
        raise BadRequest("Missing query")

    if not use_embeddings:
        scoring_type = 'TYPES'

    elif 'norm' in cosine_function.lower():
        scoring_type = 'COSINE_NORM'

    elif 'abs' in cosine_function.lower():
        scoring_type = 'COSINE_ABS'

    else:
        scoring_type = 'COSINE_ANG'

    for row in query.split('#'):
        tuple = list()

        for column in row.split('<>'):
            tuple.append(column)

        query_table.append(tuple)

    conn = Connector(host)
    template = loader.get_template('jdlc/index.html')
    context = {}

    if not conn.isConnected():
        context = {
            'error': 'Could not connect to Jazero instance. Make sure all Jazero services are running.'
        }

    else:
        result = conn.search(100, scoring_type, query_table)
        j = json.loads(result)['scores']
        tables = list()

        for entry in j:
            tables.append(entry['table ID'])

        context = {
            'k': '100',
            'result': tables
        }

    return HttpResponse(template.render(context, request))
