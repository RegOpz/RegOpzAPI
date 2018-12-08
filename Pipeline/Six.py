import operator

def iterkeys(d, **kw):
    return iter(d.keys(**kw))

def itervalues(d, **kw):
    return iter(d.values(**kw))

def iteritems(d, **kw):
    return iter(d.items(**kw))

def iterlists(d, **kw):
    return iter(d.lists(**kw))

viewkeys = operator.methodcaller("keys")

viewvalues = operator.methodcaller("values")

viewitems = operator.methodcaller("items")