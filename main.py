# Autor: Kevin Adolfo Carvalho Koberstain de Araujo
# Github: https://github.com/kevinadolf

import threading
import time
import random
from queue import Queue

# tpicos predefinidos
TOPICS = ["Esportes", "Noticias", "Tecnologia"]

# broker
class Broker:
    def __init__(self):
        self.topics = {topic: Queue() for topic in TOPICS}  # filas de mensagens para cada tópico
        self.subscribers = {topic: [] for topic in TOPICS}  # lista de assinantes para cada tópico
        self.lock = threading.Lock() # .lock evita race conditions -> 2 threads ao mesmo tempo 

    def publish(self, topic, message):
        with self.lock:
            if topic in self.subscribers and self.subscribers[topic]: # se assinantes==true, ou seja, se tiver assinantes
                print(f"Enviando mensagem em '{topic}': {message}")
                self.topics[topic].put(message)  # add a mensagem na fila do topico
            else:
                print(f"Sem assinantes para o tópico '{topic}'. Mensagem descartada.")

    def subscribe(self, topic, subscriber):
        with self.lock:
            if topic in self.subscribers:
                self.subscribers[topic].append(subscriber)
                print(f"{subscriber.name} se inscreveu no tópico '{topic}'.")

    def distribute_messages(self):
        while True:
            with self.lock:
                for topic, queue in self.topics.items():
                    if not queue.empty():
                        message = queue.get()  # get mensagem da fila do tópico
                        for subscriber in self.subscribers[topic]:
                            subscriber.receive_message(message)  # 'entrega' a mensagem para cada assinante

                time.sleep(0.5)

# publicadores
class Publisher(threading.Thread):
    def __init__(self, name, broker):
        threading.Thread.__init__(self)
        self.name = name
        self.broker = broker

    def run(self):
        while True:
            topic = random.choice(TOPICS)  # topico aleatorio
            message = f"Mensagem de {self.name} em '{topic}'"
            self.broker.publish(topic, message)  # publicando a mensagem
            time.sleep(2)

# assinantes
class Subscriber(threading.Thread):
    def __init__(self, name, broker, topic):
        threading.Thread.__init__(self)
        self.name = name
        self.broker = broker
        self.topic = topic

    def run(self):
        self.broker.subscribe(self.topic, self)  # assinando um tópico
        while True: #tempo pra esperar mensagens
            time.sleep(1)

    def receive_message(self, message):
        print(f"{self.name} recebeu: {message}")

# funcao para rodar o sistema
def main():
    broker = Broker()

    # criando e iniciando publicadores
    publishers = [Publisher(f"Publicador-{i+1}", broker) for i in range(3)]
    for publisher in publishers:
        publisher.start()

    # criando e iniciando assinantes
    subscribers = [
        Subscriber(f"Assinante-{i+1}", broker, random.choice(TOPICS)) for i in range(5) #aleatorizando escolha de topico
    ]
    for subscriber in subscribers:
        subscriber.start()

    # broker para distribuir as mensagens
    broker_thread = threading.Thread(target=broker.distribute_messages)
    broker_thread.start()

if __name__ == "__main__":
    main()