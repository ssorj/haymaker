import sqlite3 as _sqlite

from brbn import *
from pencil import *

class _Application(BrbnApplication):
    def receive_request(self, request):
        request.database_connection = _sqlite.connect("data/data.sqlite") # XXX

        try:
            if request.path_info in ("/", "/index.html"):
                return self.send_index(request)
        finally:
            request.database_connection.close()

        return request.respond_not_found()
        
    def send_index(self, request):
        cursor = request.database_connection.cursor()
        statement = "select from_, substr(subject, 0, 80), date from messages limit 200"
        content = list()

        cursor.execute(statement)

        records = cursor.fetchall()
        content = html_table(records, False)
                
        return request.respond_ok(content, "text/html")
    
app = _Application()
