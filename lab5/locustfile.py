from locust import HttpUser, task

class StaticWebUser(HttpUser):
    @task
    def load_index(self):
        self.client.get("/")

    @task
    def load_second(self):
        self.client.get("/index2.html")

    @task
    def load_404(self):
        self.client.get("/unknown.html")