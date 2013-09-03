

def sanitize_document_keys(doc):
    if isinstance(doc, dict):
        for k in doc.keys():
            doc[k] = sanitize_document_keys(doc[k])
            if k.startswith('$'):
                doc['_%s' % (k)] = doc.pop(k)
    if isinstance(doc, list):
        for e in doc:
            doc = sanitize_document_keys(e)
    return doc
