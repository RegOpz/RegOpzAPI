from Helpers.DatabaseHelper import DatabaseHelper
class Document(object):
    def __init__(self, params=None):
        if params != None:
            self.id = params['id']
            self.file = params['file']
            self.uploaded_by = params['uploaded_by']
            self.time_stamp = params['time_stamp']
            self.ip = params['ip']
            self.comment = params['comment']
        else:
            self.id = None
            self.file = None
            self.uploaded_by = None
            self.time_stamp = None
            self.ip = None
            self.comment = None
    def add(self):
        dbhelper = DatabaseHelper()
        values = (self.id, self.file, self.uploaded_by, self.time_stamp, self.ip, self.comment)
        queryString = "INSERT INTO Document (id, file,uploaded_by,time_stamp,ip,comment) VALUES (%s, %s, %s, %s, %s, %s)"
        rowid = dbhelper.transact(queryString,values)
        self.get(rowid)
        return self.__dict__
    def get(self,documentId=None):
        dbhelper = DatabaseHelper()
        if documentId != None:
            self.id = documentId
            queryParams = (self.id)
            queryString = "SELECT * FROM Document WHERE id = %s"
            print(queryString % queryParams)
            documents = dbhelper.query(queryString,queryParams)
            if documents.rowcount != 0:
                document = documents.fetchone()
                print(document)
                self.file = document[1]
                self.uploaded_by = document[2]
                self.time_stamp = str(document[3])
                self.ip = document[4]
                self.comment = document[5]
                return self.__dict__
            return None
        else :
            documentList = []
            queryString = "SELECT * FROM Document WHERE 1"
            print(queryString)
            documents = dbhelper.query(queryString)
            if documents.rowcount != 0:
                allDocuments = documents.fetchall()
                print(allDocuments)
                for (docid, filename,  uploaded_by, time_stamp, ip, comment) in allDocuments:
                    document = Document({
                        'id': docid,
                        'file': filename,
                        'uploaded_by': uploaded_by,
                        'time_stamp': str (time_stamp),
                        'ip': ip,
                        'comment': comment
                    })
                    documentList.append(document.__dict__)
                return documentList
            return None
