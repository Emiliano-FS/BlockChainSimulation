from Blockchain import *
from simian import Simian
import random, math, argparse

parser = argparse.ArgumentParser(
    description='PlumTree + DIMPLE Protocol Simulation.',
    formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument('total_nodes', metavar='NNODES', type=int,
                    help='total number of nodes')
parser.add_argument('endtime', metavar='ENDTIME', type=float,
                    help='simulation end time')
parser.add_argument("-l", "--lookahead", type=float, metavar='LOOKAHEAD', default=0.1,
                    help="min delay of mailboxes -> default 0.1")
parser.add_argument("--seedR", type=int, metavar='SEED', default=10,
                    help="seed for random number generation -> default 10")
parser.add_argument("--useMPI", type=int, metavar='MPI', default=0,
                    help="use mpi -> 0-false  1-true")
parser.add_argument("--activeChurn", type=int, metavar='CHURN', default=0,
                    help="activates the network churn-> default 0")
parser.add_argument("--failRate", type=float, metavar='FAILRATE', default=0.0,
                    help="node fail rate [0.0 ... 1.0]")

###### DIMPLE
parser.add_argument("--shuffleTime", type=float, metavar='TIME', default=25,
                    help="Time to trigger the shuffle -> default 25")

args = parser.parse_args()

uMPI = False
if args.useMPI == 1:
    uMPI = True

## PLUMTREE variables
nodes = args.total_nodes
lookahead = args.lookahead
random.seed(args.seedR)
churn = args.activeChurn
triggerSysReportTime = args.endtime - 1
timeout1 = args.lookahead
timeout2 = args.lookahead / 2
threshold = 3

timerPTacks = 2.5
failRate = args.failRate
delayLazy = 1

# DIMPLE variables
shuffleTime = args.shuffleTime
shuffleSize = math.floor(math.log(nodes,10)) 
maxActiveView = math.ceil(math.log(args.total_nodes,10)) + 1
maxPassiveView = math.ceil(math.log(args.total_nodes,10) + 1) * 6
maxPartialView = maxActiveView + maxPassiveView
dimpleTimer = 1

downNodes = []
upNodes = []

name = "PlumTreeDIMPLE/PlumTree + DIMPLE" + str(args.total_nodes)+'-Seed'+str(args.seedR)+'-ShuffleTime'+str(args.shuffleTime)+'-LOOKAHEAD'+str(args.lookahead)+'-CHURN'+str(args.activeChurn)

simName, startTime, endTime, minDelay, useMPI, mpiLib = name, 0, args.endtime, 0.00001, uMPI, "/usr/lib/x86_64-linux-gnu/libmpich.so"
simianEngine = Simian(simName, startTime, endTime, minDelay, useMPI)


class msgGossip:
    def __init__(self,type,ptype,m,mID,round,sender):
        self.type = type
        self.payloadType = ptype
        self.payload = m
        self.ID = mID
        self.round = int(round)
        self.sender = int(sender)

    def toString(self):
        return '%s-%s-%d-%d-%d'%(self.type,self.payload,self.ID,self.round,self.sender)
    
class msgDimple:
    def __init__(self,type,payload,sender):
        self.type = type
        self.payload = payload
        self.sender = sender

    def toString(self):
        return '%s-%d-%d-%d'%(self.type,self.msgs)
    
class partialViewEntry:
    def __init__(self,node_idx,age,visited):
        self.node_idx = node_idx
        self.age = age
        self.visited = visited

    def __str__(self):
        return f"partialViewEntry(node_idx={self.node_idx}, age={self.age}, visited={self.visited})"

    # Optional: for better printing in lists or debugging
    def __repr__(self):
        return self.__str__()
    
class msgReport:
    def __init__(self, type, msgs, degree):
        self.type = type
        self.msgs = msgs
        self.degree = degree

class ChurnManager(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(ChurnManager, self).__init__(baseInfo)
        self.reqService(100, "do_churn_cycle", "none")

    def do_churn_cycle(self, *args):
        churn_size = int(nodes * failRate)
        # Churn out
        if len(upNodes) > churn_size:
            to_churn = random.sample(upNodes, churn_size)
            for node_id in to_churn:
                delay = random.expovariate(1/20)
                self.reqService(delay, "force_churn_out", "", "Node", node_id)

class ReportNode(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(ReportNode, self).__init__(baseInfo)

        #report variables
        self.reliability = {}
        self.latency = {}
        self.redundancy = {}
        self.degree = 0
        self.totalMiners = 0
        self.totalTransactions = 0 
        self.longestChain = []
        self.chainLenghts = 0
        self.maxDegree = 0
        self.minDegree = 1000
        self.shortestPath = 0
        self.reqService(endTime, "PrintSystemReport", "none")

    def SystemReport(self,*args):
        payload = args[0]
        msg = payload[0]
        trx = payload[1]
        chain = payload[2]
        ifminer = payload[3]

        if msg.degree > self.maxDegree:
            self.maxDegree = msg.degree
        if msg.degree < self.minDegree:
            self.minDegree = msg.degree

        self.degree += msg.degree

        for m in msg.msgs:
            self.shortestPath += m[1]
            id = m[0]
            if id not in self.latency.keys() or self.latency[id] < m[1]:
                self.latency[id] = m[1]
            if id not in self.reliability.keys():
                self.reliability[id] = 0
            if id not in self.redundancy.keys():
                self.redundancy[id] = [0,0,0]

            self.reliability[id] += 1
            self.redundancy[id][0] += m[2]
            self.redundancy[id][1] += m[3]
            self.redundancy[id][2] += m[4]
        
        self.totalTransactions += trx

        if len(self.longestChain) < len(chain):
            self.longestChain = chain
        
        self.chainLenghts += len(chain)
        
        if ifminer : self.totalMiners += 1

    def PrintSystemReport(self,*args):
        degree = round(self.degree / (nodes * (1 - failRate)),2)
        #self.out.write("Degree:%.2f\n"%(degree))
        avRel = 0
        avNodes = 0
        avLat = 0
        avRmr = 0
        avGossip = 0
        avIhave = 0
        avGraft = 0
        
        avRel10 = 0
        count = 0

        for id in sorted(self.reliability.keys()):
            r = self.reliability[id]
            reliability = round(r / (nodes * (1-failRate)) * 100,3)
            lat = self.latency[id]
            if r <= 1:
                rmr = 0
            else:
                rmr = round((self.redundancy[id][0] / (r - 1)) - 1,3)

            avRel += reliability
            avNodes += r
            avLat += lat
            avRmr += rmr
            avGossip += self.redundancy[id][0]
            avIhave += self.redundancy[id][1]
            avGraft += self.redundancy[id][2]

            self.out.write("%s--Reliability:%.3f%%    Nodes:%d    Latency:%d   RMR:%.3f        Gossip:%d   Ihave:%d   Graft:%d\n\n"%(id,reliability,r,lat,rmr,self.redundancy[id][0],self.redundancy[id][1],self.redundancy[id][2]))
            if reliability > avRel10:
                avRel10 = reliability
                count += 1

            if count % 10 == 0:
                id = count / 10
                avRel10 /= 10
                #self.out.write("%s--Reliability:%.3f%%\n\n"%(id,avRel10))
                avRel10 = 0


        msgs = len(self.reliability.keys())
        if msgs > 0:
            avRel /= msgs
            avNodes /= msgs
            avLat /= msgs
            avRmr /= msgs
            avGossip /= msgs
            avIhave /= msgs
            avGraft /= msgs
            self.shortestPath /= msgs
            self.shortestPath /= (nodes * (1-failRate))
            average_length = self.chainLenghts / (nodes * (1-failRate))
            avChainL = average_length /  len(self.longestChain) * 100


            self.out.write("Number of Miners:%d   Total Transactions:%d    Longest Chain:%d     Avarage Chain lenght:%f\n"%(self.totalMiners,self.totalTransactions,len(self.longestChain),avChainL))
            self.out.write("AVERAGE--Reliability:%.3f%%    Nodes:%d    Latency:%.1f   RMR:%.3f        Gossip:%d   Ihave:%d   Graft:%d\n\n"%(avRel,avNodes,avLat,avRmr,avGossip,avIhave,avGraft))
        self.out.write("Degree:%.2f  min:%d    max:%d    shortest path:%.2f\n"%(degree,self.minDegree,self.maxDegree,self.shortestPath))



class Node(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(Node, self).__init__(baseInfo)
        self.total_nodes = int(args[1])
        self.node_idx = int(args[0])
        self.miner = False
        self.trxMade = 0

        self.active = True
        self.blockchain = Blockchain()
        self.blockchain.create_genesis_block()
        self.mining = False

        #plumTree variables
        self.eagerPushPeers = []
        self.lazyPushPeers = []
        self.lazyQueues = []
        self.missing = []
        self.receivedMsgs = {}
        self.timers = []
        self.timersAck = {}

        #Report Variables
        self.report = {}


        #DIMPLE Variables
        self.partial_view = []
        self.timerDimple = []
        delay = 10 / self.total_nodes
        delay2 = delay * self.node_idx
        if self.node_idx < 3:  # Seed nodes 0, 1, 2
            peer_ids = random.sample([i for i in range(3) if i != self.node_idx], 2)
            for peer_id in peer_ids:
                visited_list = [peer_id, self.node_idx]  # Simulate movement between them
                self.partial_view.append(partialViewEntry(peer_id, 0, list(visited_list)))    
                self.reqService( delay2 + 1 , "DimpleShuffle", "none")  
        else:
            contactNode = 0 
            msg = msgDimple('JOIN',[],self.node_idx)
            self.reqService( delay2, "Dimple", msg, "Node", contactNode)  

        self.reqService(endTime - 1, "TriggerSystemReport", "none")
                                  

   #--------------------------------------- GOSSIP ---------------------------------------------------

    def PlumTreeGossip(self, *args):
        msg = args[0]
        #self.out.write(str(self.engine.now) + (":%d rcvd msg '%s' %d\n" % (self.node_idx, msg.type,msg.sender)))
        if self.active==True:
            if msg.type =='PRUNE':
                if msg.sender in self.timersAck.keys():
                    val = False
                    for pair in self.timersAck[msg.sender]:
                        if val == False and pair[0] == msg.ID and pair[1] == msg.round:
                             self.timersAck[msg.sender].remove(pair)
                             val = True

                if msg.sender in self.eagerPushPeers:
                    self.eagerPushPeers.remove(msg.sender)
                if msg.sender not in self.lazyPushPeers and any(entry.node_idx == msg.sender for entry in self.partial_view):
                    self.lazyPushPeers.append(msg.sender)

            elif msg.type =='IHAVE':
                msgToSend = msgGossip('ACK','','' ,msg.ID,msg.round,self.node_idx)
                self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

                if msg.ID not in self.report.keys():
                    self.report[msg.ID] = [0,1,0]
                else:
                    self.report[msg.ID][1] += 1

                if msg.ID not in self.receivedMsgs.keys():
                    self.missing.append((msg.ID,msg.sender,msg.round))
                    # setup timer
                    if msg.ID not in self.timers:
                        self.timers.append(msg.ID)
                        self.reqService(timeout1, "Timer", msg.ID)

            elif msg.type =='GRAFT':
                if msg.ID not in self.report.keys():
                    self.report[msg.ID] = [0,0,1]
                else:
                    self.report[msg.ID][2] += 1

                if msg.sender not in self.eagerPushPeers and any(entry.node_idx == msg.sender for entry in self.partial_view):
                    self.eagerPushPeers.append(msg.sender)
                if msg.sender in self.lazyPushPeers:
                    self.lazyPushPeers.remove(msg.sender)
                if msg.ID in self.receivedMsgs.keys():
                    msgToSend = msgGossip('GOSSIP', self.receivedMsgs[msg.ID].payloadType ,self.receivedMsgs[msg.ID].payload,msg.ID,msg.round,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

            elif msg.type =='GOSSIP':
                if msg.ID not in self.report.keys():
                    self.report[msg.ID] = [1,0,0]
                else:
                    self.report[msg.ID][0] += 1

                if msg.ID not in self.receivedMsgs.keys():
                    msgToSend = msgGossip('ACK','','',msg.ID,msg.round,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

                    self.receivedMsgs[msg.ID] = msg

                    if msg.ID in self.timers:
                        self.timers.remove(msg.ID)

                    if msg.payloadType  == "BLOCK":
                        block = Block.from_dict(msg.payload)
                        accepted = self.blockchain.consensus(block)
                        if accepted:
                            self.blockchain.remove_confirmed_transactions(block)
                            if self.mining:
                                self.mining = False

                        self.LazyPush(msg)
                        self.EagerPush(msg)            

                    elif msg.payloadType  == "TRX":
                        if self.miner:
                            self.blockchain.add_new_transaction(msg.payload)
                            if self.miner and not self.mining and len(self.blockchain.unconfirmed_transactions) >= 100:
                                self.mining = True
                                avg_mining_time = 30
                                delay = random.expovariate(1/avg_mining_time)
                                self.reqService(delay, "mine_block", "none")

                        self.LazyPush(msg)
                        self.EagerPush(msg)

                    if msg.sender not in self.eagerPushPeers and any(entry.node_idx == msg.sender for entry in self.partial_view):
                        self.eagerPushPeers.append(msg.sender)
                    if msg.sender in self.lazyPushPeers:
                        self.lazyPushPeers.remove(msg.sender)
                else:
                    if msg.sender in self.eagerPushPeers:
                        self.eagerPushPeers.remove(msg.sender)
                    if msg.sender not in self.lazyPushPeers and any(entry.node_idx == msg.sender for entry in self.partial_view):
                        self.lazyPushPeers.append(msg.sender)

                    msgToSend = msgGossip('PRUNE','','',msg.ID,msg.round,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

            elif msg.type =='BROADCAST':
                mID = msg.ID
                msg.type = 'GOSSIP'
                self.EagerPush(msg)
                self.LazyPush(msg)
                self.receivedMsgs[mID] = msg
                self.report[msg.ID] = [0,0,0]

            elif msg.type =='ACK':
                #remover lista de timers
                if msg.sender in self.timersAck.keys():
                    val = False
                    for pair in self.timersAck[msg.sender]:
                        if val == False and pair[0] == msg.ID and pair[1] == msg.round:
                             self.timersAck[msg.sender].remove(pair)
                             val = True


    def mine_block(self, *args):
        if not self.active or not self.mining:
            return
        self.blockchain.mine()
        new_block = self.blockchain.last_block
        block_id = "B-" +str(random.randint(11111111,99999999))
        block_msg = msgGossip('GOSSIP',"BLOCK", new_block.to_dict(), block_id, 0, self.node_idx)        
        self.receivedMsgs[block_msg.ID] = block_msg
        self.report[block_msg.ID] = [1, 0, 0]
        self.LazyPush(block_msg)
        self.EagerPush(block_msg)
        self.mining = False

    def EagerPush(self, msg):
        sender = msg.sender
        msgToSend = msgGossip('GOSSIP',msg.payloadType ,msg.payload,msg.ID,msg.round + 1,self.node_idx)        
        for n in self.eagerPushPeers:
            if n != sender:
                #create timer to receive ack
                if n not in self.timersAck.keys():
                    self.timersAck[n] = []
                self.timersAck[n].append((msg.ID, msg.round + 1))
                self.reqService(lookahead * timerPTacks, "TimerAcks", (n, msg.ID, msg.round + 1))
                self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", n)

    def LazyPush(self, msg):
        sender = msg.sender
        msgToSend = msgGossip('IHAVE','',msg.payload,msg.ID,msg.round + 1,self.node_idx)
        for n in self.lazyPushPeers:
            if n != sender:
                #create timer to receive ack
                if n not in self.timersAck.keys():
                    self.timersAck[n] = []
                self.timersAck[n].append((msg.ID, msg.round + 1))
                self.reqService(lookahead * delayLazy * timerPTacks, "TimerAcks", (n, msg.ID, msg.round + 1))
                self.reqService(lookahead * delayLazy, "PlumTreeGossip", msgToSend, "Node", n)

    def Optimization(self, mID, round, sender):
        val = True
        for pair in self.missing:
            if val and pair[0] == mID:
                if pair[2] < round and round - pair[2] >= threshold:
                    val = False
                    msgToSend = msgGossip('PRUNE','','',mID,round,self.node_idx)
                    msgToSend = msgGossip('GRAFT','','',mID,round,self.node_idx)

    def NeighborUP(self, node):
        if node not in self.eagerPushPeers:
            self.eagerPushPeers.append(node)

    def NeighborDown(self, node):
        if node in self.eagerPushPeers:
            self.eagerPushPeers.remove(node)
        if node in self.lazyPushPeers:
            self.lazyPushPeers.remove(node)

        for pair in self.missing:
            if pair[1] == node:
                self.missing.remove(pair)

    def Timer(self,*args):
        mID = args[0]
        if mID in self.timers:
            m = (mID,0,0)
            val = True
            for p in self.missing:
                if p[0] == mID and val:
                    m = p
                    val = False
                    self.missing.remove(p)

            if val == False:
                if m[1] not in self.eagerPushPeers and any(entry.node_idx == m[1] for entry in self.partial_view):
                    self.eagerPushPeers.append(m[1])
                if m[1] in self.lazyPushPeers:
                    self.lazyPushPeers.remove(m[1])

                msgToSend = msgGossip('GRAFT','','',mID,m[2],self.node_idx)
                self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", m[1])

                self.reqService(timeout2, "Timer", mID)

    def TimerAcks(self,*args):
        pair = args[0]
        dest = pair[0]
        mID = pair[1]
        round = pair[2]
        if dest in self.timersAck.keys():
            val = False
            for pair in self.timersAck[dest]:
                if val == False and pair[0] == mID and pair[1] == round:
                     self.timersAck[dest].remove(pair)
                     val = True

            if val == True:
                #self.out.write("ack n chegou no node:"+ str(self.node_idx)+"\n")
                self.NodeFailure(dest)
                self.timersAck[dest] = []

                self.NeighborDown(dest)

#--------------------------------------- PEER SELECTION ---------------------------------------------------
    
    def Dimple(self, *args):
        msg = args[0]
        if self.active:
            if msg.type == 'VIEW_EXCHANGE_REQUEST':
                # Q receives P's subset, replies with its own
                own_subset = random.sample(self.partial_view, min(shuffleSize, len(self.partial_view)))
                self.ExchangeProcedure(msg.payload, own_subset)
                reply = msgDimple("VIEW_EXCHANGE_RESPONSE", [own_subset, msg.payload], self.node_idx)
                self.reqService(lookahead, "Dimple", reply, "Node", msg.sender)
                
            elif msg.type == 'VIEW_EXCHANGE_RESPONSE':
                self.timerDimple.remove(msg.sender)
                received_subset, sent_subset = msg.payload
                self.ExchangeProcedure(received_subset, sent_subset)

            elif msg.type == 'REINFORCEMENT':
                if len(self.partial_view) > 0:
                    node_toreinforce = max(self.partial_view, key=lambda x: x.age)
                    self.timerDimple.append(node_toreinforce.node_idx)
                    self.reqService(dimpleTimer, "TimerDimple", node_toreinforce.node_idx)
                    dimpleMsg = msgDimple('REINFORCEMENT_INITIATE',self.node_idx, self.node_idx)
                    self.reqService(lookahead, "Dimple", dimpleMsg , "Node", node_toreinforce.node_idx)

            elif msg.type == 'REINFORCEMENT_INITIATE':
                response = self.ReinforcementInitiateProcedure(msg.payload)
                dimpleMsg = msgDimple('REINFORCEMENT_RESPONSE',[response, msg.payload] , self.node_idx)
                self.reqService(lookahead, "Dimple", dimpleMsg , "Node", msg.sender)
                
            elif msg.type == 'REINFORCEMENT_RESPONSE':
                response_entry, entry_to_replace = msg.payload
                self.timerDimple.remove(msg.sender)
                self.ReinforcementResponseProcedure(response_entry,entry_to_replace)

            elif msg.type == 'JOIN':
                # JOIN Request → Q returns local view built from visited[-2] or fallback
                local_view = []
                for entry in self.partial_view:
                    if len(entry.visited) >= 2:
                        local_view.append(entry.visited[-2])
                    else:
                        local_view.append(entry.node_idx)
                
                # Send join_candidates to P
                response = msgDimple("JOIN_RESPONSE", local_view, self.node_idx)
                self.reqService(lookahead, "Dimple", response, "Node", msg.sender)
                
            elif msg.type == 'JOIN_RESPONSE':
                # Node P gets view, performs reinforcements
                candidates = msg.payload
                random.shuffle(candidates)

                #for candidate in candidates: 
                #    if candidate != self.node_idx:
                #        self.partial_view.append(partialViewEntry(candidate, 0, [self.node_idx]))
          
                for peer_id in candidates:
                    if peer_id != self.node_idx:
                        self.timerDimple.append(peer_id)
                        self.reqService(dimpleTimer, "TimerDimple", peer_id)
                        reinforce_msg = msgDimple("REINFORCEMENT_INITIATE", self.node_idx,self.node_idx)
                        self.reqService(lookahead, "Dimple", reinforce_msg, "Node", peer_id)

                delay = lookahead * (len(candidates) + 2)
                self.reqService(delay, "DimpleShuffle", "none") 
                
    
    def DimpleShuffle(self, *args):
        if not self.active or len(self.partial_view) <= 0:
            self.reqService(shuffleTime, "DimpleShuffle", "none")  # Reschedule anyway
            return

        # Age all entries
        for entry in self.partial_view:
            entry.age += 1

        # Select the oldest entry as the target Q
        oldest_entry = max(self.partial_view, key=lambda e: e.age)

        # Select l-1 random peers + self
        eligible = [e for e in self.partial_view if e.node_idx != oldest_entry.node_idx]
        subset = random.sample(eligible, min(shuffleSize - 1, len(eligible)))
        subset.append(partialViewEntry(self.node_idx, 0, [self.node_idx]))

        # Set timeout
        self.timerDimple.append(oldest_entry.node_idx)
        self.reqService(dimpleTimer, "TimerDimple", oldest_entry.node_idx)

        # Send VIEW_EXCHANGE_REQUEST to Q
        msg = msgDimple("VIEW_EXCHANGE_REQUEST", subset, self.node_idx)
        self.reqService(lookahead, "Dimple", msg, "Node", oldest_entry.node_idx)
        
        # Repeat Reinforcement l times in parallel.
        for i in range(shuffleSize):
            msg = msgDimple("REINFORCEMENT", None , self.node_idx)
            self.reqService(lookahead, "Dimple", msg, "Node", self.node_idx)

        # Reschedule next shuffle
        self.reqService(shuffleTime, "DimpleShuffle", "none")


    def ReinforcementInitiateProcedure(self, received_entry_id):
        if received_entry_id == self.node_idx:
            return None  # Don't reinforce with self

        existing_ids = {entry.node_idx for entry in self.partial_view}

        # If already present, ignore
        if received_entry_id in existing_ids:
            return None

        # If space available, add directly
        if len(self.partial_view) < maxPartialView:
            new_entry = partialViewEntry(received_entry_id, 0, [received_entry_id, self.node_idx])
            self.partial_view.append(new_entry)
            self.UpdatePlumTreePeers()
            return None  # No eviction needed

        # Replace a random entry
        to_replace = random.randrange(len(self.partial_view))
        evicted_entry = self.partial_view[to_replace]
        self.partial_view[to_replace] = partialViewEntry(received_entry_id, 0, [received_entry_id, self.node_idx])
        self.UpdatePlumTreePeers()
        return evicted_entry  # Send this back to P
    
    def ReinforcementResponseProcedure(self, response_entry, entry_to_replace_id):
        if response_entry is None:
            return  # Nothing to update
        
        # If P receives a valid response, P adds the node information to an empty slot if there is one
        if len(self.partial_view) < maxPartialView:
            self.partial_view.append(response_entry)
            self.UpdatePlumTreePeers()
            return

        # Or replaces the entry of Q with the received information of another node.
        for i, entry in enumerate(self.partial_view):
            if entry.node_idx == entry_to_replace_id:
                self.partial_view[i] = response_entry
                self.UpdatePlumTreePeers()


    def ExchangeProcedure(self, received_subset, sent_subset):
        current_ids = {entry.node_idx for entry in self.partial_view}
    
        for received_entry in received_subset:
            if received_entry.node_idx == self.node_idx or received_entry.node_idx in current_ids:
                continue
            
            new_visited = received_entry.visited.copy()
            new_visited.append(self.node_idx)
            new_entry = partialViewEntry(received_entry.node_idx, 0, new_visited)
    
            if len(self.partial_view) < maxPartialView:
                self.partial_view.append(new_entry)
                break
            
            for i, e in enumerate(self.partial_view):
                if any(entry.node_idx == e.node_idx for entry in sent_subset):
                    self.partial_view[i] = new_entry
    
        self.UpdatePlumTreePeers()


    def NodeFailure(self, node_id):
        self.partial_view = [entry for entry in self.partial_view if entry.node_idx != node_id]
        self.NeighborDown(node_id)
        
    def TimerDimple(self, *args):
        dest = args[0]
        if dest in self.timerDimple:
            self.timerDimple.remove(dest)
            self.NodeFailure(dest)

    def UpdatePlumTreePeers(self):
        current_peers = [entry.node_idx for entry in self.partial_view if entry.node_idx != self.node_idx]

        # Reset eager from partial_view
        self.eagerPushPeers = current_peers

        # Retain lazy peers only if they still exist in partial_view
        self.lazyPushPeers = [peer for peer in self.lazyPushPeers if peer in current_peers]

 #--------------------------------------- TRIGGERS ---------------------------------------------------#   
      
    def TriggerSystemReport(self,*args):
        report = []
        degree = len(self.eagerPushPeers)+len(self.lazyPushPeers)
        for m in self.receivedMsgs.keys():
            report.append((m,self.receivedMsgs[m].round,self.report[m][0],self.report[m][1],self.report[m][2]))
        msgToSend = msgReport('reply',report,degree)
        self.reqService(lookahead, "SystemReport", [msgToSend, self.trxMade , self.blockchain.chain, self.miner] , "ReportNode", 0)

    def BecomeMiner(self, *args):
        self.miner = True

    def force_churn_out(self, *args):
        if self.active:
            self.active = False
            self.peers = []
            self.blockchain.chain = []
            self.blockchain.forks = {}
            self.blockchain.orphans = []
            self.receivedMsgs = {}
            self.report = {}

            if self.node_idx in upNodes:
                upNodes.remove(self.node_idx)
            if self.node_idx not in downNodes:
                downNodes.append(self.node_idx)

    def create_transaction(self,*args):
        n = random.choice(upNodes)
        avg_transactionT = .7
        delay = random.expovariate(1/avg_transactionT)
        if self.active and not self.miner:
            transaction = Transaction(self.node_idx)
            tx_msg = msgGossip('BROADCAST',"TRX", transaction, transaction.trans_id, 0, self.node_idx)
            self.reqService(lookahead, "PlumTreeGossip", tx_msg, "Node", self.node_idx)
            self.trxMade += 1
            self.reqService(delay , "create_transaction", "" , "Node", n)
        else:
            self.reqService(delay , "create_transaction", "" , "Node", n)


for i in range(0, nodes):
    simianEngine.addEntity("Node", Node, i, i, nodes)

simianEngine.addEntity("ReportNode", ReportNode, 0, 0)

if churn:
    simianEngine.addEntity("ChurnManager", ChurnManager, 0, 0)

for i in range(0, nodes):
    upNodes.append(i)

for i in range(0, math.ceil((0.01 * nodes))):
    n = random.choice(upNodes)
    simianEngine.schedService(lookahead, "BecomeMiner", "", "Node", n)
    upNodes.remove(n)

n = random.choice(upNodes)

simianEngine.schedService(50 + lookahead , "create_transaction","" , "Node", n)


simianEngine.run()
simianEngine.exit()