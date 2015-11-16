from brbn import *

class TestApplication(BrbnApplication):
    def receive_request(self, request):
        return request.respond("200 OK", "Test!", "text/plain")

app = TestApplication()
