import sys
import math
import random
import numpy as np
from heapq import heappush as push, heappop as pop

INFINITY = float('infinity')

def ExponentialVariable(lambda_):
    def ExponentialVar():
        return np.random.exponential(scale=1/lambda_)

    return ExponentialVar

def NormalVariable(mu = 0, sigma = 1):
    def NormalVar():
        return np.random.normal(mu, sigma)

    return NormalVar

# este método desplaza el horario en que se realizará el evento hacia un horario laborable, preferiblemente 8am
def put_eight_am (time):
    day_actual = int(time/1440) + (0 if time%1440 == 0 else 1)
    result = 0

    if time < 480:
        result = (day_actual-1)*1440 + 480
    else:
        result = day_actual*1440 + 480 + time - (int(time/1440)*1440 + 1200)

    return result

class Process:
    def __init__(self, id, time):
        self.id = id
        self.time = time

class Ship:
    def __init__(self, id, size, wait_time = 0, arrival_time = 0):
        self.id = id # id del barco
        self.size = size #tamaño del barco
        self.wait_time = wait_time # tiempo de espera del barco
        self.arrival_time = arrival_time # tiempo en el que arribó el barco
        self.process = -1 # id del proceso asociado a dicho barco 

class Dam:
    def __init__(self):
        self.queue = [] # cola de barcos de espera para entrar al dique
        self.inside = [] # barcos que se encuentran en el interior dediqueeu

        ## self.start_time se refiere al momento de entrada de los diques y self.finish_time al tiempo de salida de los barcos del dique 
        self.start_time = INFINITY
        self.finish_time = INFINITY

    # este método guarda a los barcos en el dique correspondiente y devuelve el tiempo que se demoró en entrar a los barcos al dique
    def get_ships_into(self, number_process, lambda_):
        self.inside = []
        first_row = 0 #inicializamos la primera fila
        second_row = 0 #inicializamos la segunda fila

        process_lifespan = 0 # tiempo que durará el proceso

        queue_copy = []
        for ship in self.queue:
            queue_copy.append(ship)

        for ship in queue_copy:
            if first_row + ship.size <= 6:
                #ponerlo en la primera fila
                first_row = first_row + ship.size
            elif second_row + ship.size <= 6:
                #ponerlo en la segunda fila
                second_row = second_row + ship.size
            elif first_row == second_row == 6:
                #el dique está lleno
                break
            else:
                #este barco no cabe en el dique
                continue
            
            ship.process = number_process
            self.inside.append(ship)

            #generar el tiempo que demora este barco en entrar al dique, y contarlo
            process_lifespan = process_lifespan + ExponentialVariable(lambda_)()

            #quitarlo de la cola
            self.queue.remove(ship)

        return process_lifespan

    def is_empty(self):
        return self.start_time == self.finish_time == INFINITY

class MaritimeCanal:

    def __init__(self, days, params_normal, params_exponential):
        self.T = days*24*60 #tiempo total de la simulación
        self.N = 5 #cantidad de diques
        self.number_process = -1  # id del ultimo proceso que comenzó
        self.process = [] # cola de procesos activos
        self.params_normal = params_normal
        self.params_exponential = params_exponential
        self.simulation_ships = []

    # función que genera el arribo de los barcos y los mete en un heap de eventos
    def _generate_ships_arrival(self):
        id_ships = 0

        for i in range(0,3):
            time = 0

            while time < self.T:
                scale_time = time%1440
                normal = 0
                if 480 <= scale_time and scale_time < 660:
                    normal = NormalVariable(matrix[i][0][0], matrix[i][0][1])()
                elif 660 <= scale_time and scale_time < 1020:
                    normal = NormalVariable(matrix[i][1][0], matrix[i][1][1])()
                elif 1020 <= scale_time and scale_time < 1200:
                    normal = NormalVariable(matrix[i][2][0], matrix[i][2][1])()
                else:
                    normal = NormalVariable(matrix[i][3][0], matrix[i][3][1])()

                if normal > 0:
                    time += normal
                    id_ships += 1

                    if time < self.T:
                        ship = Ship(id_ships, 2**i, 0, time)
                        self.event_id += 1
                        push(self.events, (ship.arrival_time, self.event_id, self._ship_arrival(ship)))   

    def _initialize(self):
        self.total_ships_through_canal = 0
        self.dams = [Dam() for i in range(self.N)] #lista de diques
        self.T_t = 0 #tiempo total de espera de los barcos
        self.events = [] # heap de eventos a ejecutar
        self.event_id = -1
        self._generate_ships_arrival()

    def _ship_arrival(self, ship):
        def _ship_arrival_i():
            print ('---------------------------------')
            print ('A new ship %s arrived at time %s' %(ship.id, ship.arrival_time))

            if self.dams[0].is_empty() and self.dams[0].queue == []:
                # calcular comienzo del proceso, después de abrir las compuertas
                exponential_var = ExponentialVariable(self.params_exponential[0])()
                start_process_time = ship.arrival_time + exponential_var

                # verificar que si el evento se realiza en horario no laborable, actualizar su horario para el otro dia
                scale_time = start_process_time%1440
                if scale_time < 480  or 1200 <= scale_time:
                    start_process_time = put_eight_am(start_process_time)
                    if scale_time < 480:  
                        start_process_time += exponential_var

                # verificar que el evento de entrada al primer dique se va a realizar dentro del tiempo de la simulación
                if start_process_time < self.T:
                    self.dams[0].start_time = start_process_time

                    # crear un nuevo proceso
                    self.number_process = self.number_process+1
                    self.process.append(Process(self.number_process, self.dams[0].start_time))

                    #generar un evento de entrada al primer dique
                    self.event_id += 1
                    push(self.events, (self.dams[0].start_time, self.event_id, self._start_dam(0, self.number_process))) 


            #añadirlo a la cola del primer dique
            self.dams[0].queue.append(ship)
        
        return  _ship_arrival_i

    def _start_dam(self, i, number_process):
        def _start_dam_i():
            print ('---------------------------------')
            print ('Starting dam %s (proceso %s) process at time %s'%(i, number_process, self.dams[i].start_time))  

            #actualizar el tiempo del proceso
            self.process[number_process].time = self.dams[i].start_time
            
            #entrar los barcos al dique
            lifespan = self.dams[i].get_ships_into(number_process, self.params_exponential[1])

            #sumar el tiempo de la fase de transporte
            lifespan = lifespan + ExponentialVariable(self.params_exponential[2])()

            #sumar el tiempo de la salida de los barcos
            for ship in self.dams[i].inside:
                lifespan = lifespan + ExponentialVariable(self.params_exponential[3])()

            transportation_time = self.process[number_process].time + lifespan

            # verificar que si el evento se realiza en horario no laborable, actualizar su horario para el otro dia
            scale_time = transportation_time%1440
            if scale_time < 480  or 1200 <= scale_time:
                transportation_time = put_eight_am(transportation_time) 

            # verificar que el evento finish_dam se va a realizar dentro del tiempo de la simulación
            if transportation_time < self.T:
                self.dams[i].finish_time = transportation_time

                #generar un evento de salida de este dique
                self.event_id += 1
                push(self.events, (self.dams[i].finish_time, self.event_id, self._finish_dam(i, number_process)))

            
            self.dams[i].start_time = INFINITY

        return _start_dam_i

    def _finish_dam(self, i, number_process):
        def _finish_dam_i():
            print ('---------------------------------')
            print ('Finishing dam %s (proceso %s) process at time %s'%(i, number_process, self.dams[i].finish_time))

            #actualizar el tiempo de la simulación
            self.process[number_process].time = self.dams[i].finish_time

            if i < self.N - 1:

                if self.dams[i+1].is_empty() and self.dams[i+1].queue == []:
                    # calcular el tiempo después de abrir las compuertas
                    open_gates_time = self.process[number_process].time + ExponentialVariable(self.params_exponential[0])()

                    # verificar que si el evento se realiza en horario no laborable, actualizar su horario para el otro dia
                    scale_time = open_gates_time%1440
                    if scale_time < 480  or 1200 <= scale_time:
                        open_gates_time = put_eight_am(open_gates_time)
                    
                    # verificar que el evento de entrada al i-ésimo + 1 dique se va a realizar dentro del tiempo de la simulación
                    if open_gates_time < self.T:
                        self.dams[i+1].start_time = open_gates_time 

                        #generar un evento de entrada al i-ésimo + 1 dique
                        self.event_id += 1
                        push(self.events, (self.dams[i+1].start_time, self.event_id, self._start_dam(i+1, number_process)))

                #poner los barcos en la cola del dique siguiente
                for ship in self.dams[i].inside:
                    self.dams[i+1].queue.append(ship)
            else:
                for ship in self.dams[i].inside:
                    ship.wait_time = self.process[number_process].time - ship.arrival_time
                    self.T_t += ship.wait_time
                    self.total_ships_through_canal += 1
                    self.simulation_ships.append(ship)
            
            self.dams[i].inside = []

            if self.dams[i].queue != []:
                if i == 0:
                    # calcular el tiempo después de abrir las compuertas
                    open_gates_time_2 = self.process[number_process].time + ExponentialVariable(self.params_exponential[0])()

                    # verificar que si el evento se realiza en horario no laborable, actualizar su horario para el otro dia
                    scale_time = open_gates_time_2%1440
                    if scale_time < 480  or 1200 <= scale_time:                   
                        open_gates_time_2 = put_eight_am(open_gates_time_2)
                    
                    # verificar que el evento de entrada al primer dique se va a realizar dentro del tiempo de la simulación
                    if open_gates_time_2 < self.T:
                        self.dams[0].start_time = open_gates_time_2

                        # crear un nuevo proceso
                        self.number_process = self.number_process+1
                        self.process.append(Process(self.number_process, self.dams[0].start_time))

                        #generar un evento de entrada al primer dique
                        self.event_id += 1
                        push(self.events, (self.dams[0].start_time, self.event_id, self._start_dam(0, self.number_process)))

                else:    
                    # calcular el tiempo después de abrir las compuertas
                    open_gates_time_1 = self.process[number_process].time + ExponentialVariable(self.params_exponential[0])()

                    # verificar que si el evento se realiza en horario no laborable, actualizar su horario para el otro dia
                    scale_time = open_gates_time_1%1440
                    if scale_time < 480  or 1200 <= scale_time:
                        open_gates_time_1 = put_eight_am(open_gates_time_1) 
                    
                    # verificar que el evento de entrada al i-ésimo dique se va a realizar dentro del tiempo de la simulación
                    if open_gates_time_1 < self.T:
                        self.dams[i].start_time = open_gates_time_1

                        #generar un evento de entrada al i-ésimo dique
                        self.event_id += 1
                        push(self.events, (self.dams[i].start_time, self.event_id, self._start_dam(i, self.dams[i].queue[0].process)))

            self.dams[i].finish_time = INFINITY
        return _finish_dam_i

    def start_simulation(self):
        print ('---------------------------------')
        print ('Starting simulation....')

        self._initialize()

        current_event = None
        while len(self.events) > 0:
            _, _, current_event = pop(self.events)
            current_event()

        print ('################################################################')
        print ('\tRESULTS OF THE SIMULATION')
        print ('################################################################')
        print ('Waiting time = %s hours' %(self.T_t / 60))
        print ('Total ships that go through tha maritime canal = %s' %(self.total_ships_through_canal))
        print ('Average waiting time for ship = %s hours' %((self.T_t / self.total_ships_through_canal)/60))


# Tabla de los parámetros de la función normal para cada barco en diferente horario
matrix =  [
    # 8-11am  11am-5pm   5-8pm    8pm-8am
    [(5,2),   (3,1),     (10,2),  (6,1)], # Pequeño
    [(15,3),  (10,5),    (20,5),  (15,4)], # Mediano
    [(45,3),  (35,7),    (60,9),  (46,6)] # Grande
]

days_simulation = 30

                            #  Lambdas:
params_exponential = [4,    # Aperturas de las compuertas
                      2,    # Entradas de los barcos al dique
                      7,    # Fase de transporte
                      1.5]  # Salida del dique
   
maritime_canal = MaritimeCanal(days_simulation, matrix, params_exponential) # days of the simulation
maritime_canal.start_simulation()


