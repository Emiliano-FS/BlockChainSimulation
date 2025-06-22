from Blockchain import * 
from simian import Simian
import random, math, argparse

parser = argparse.ArgumentParser(
    description='The PHOLD model.',
    formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument('total_nodes', metavar='NNODES', type=int,
                    help='total number of nodes')
parser.add_argument('endtime', metavar='ENDTIME', type=float,
                    help='simulation end time')
parser.add_argument("-l", "--lookahead", type=float, metavar='LOOKAHEAD', default=0.1,
                    help="min delay of mailboxes -> default 0.1")
parser.add_argument("--useMPI", type=int, metavar='MPI', default=0,
                    help="use mpi -> 0-false  1-true")
parser.add_argument("--seedR", type=int, metavar='SEED', default=10,
                    help="seed for random number generation -> default 10")
parser.add_argument("--updateViews", type=float, metavar='TIME', default=5,
                    help="update passive Views trigger time -> default 5")
parser.add_argument("--activeChurn", type=int, metavar='CHURN', default=0,
                    help="activates the network churn-> default 0")
parser.add_argument("--failRate", type=float, metavar='FAILRATE', default=0.0,
                    help="node fail rate [0.0 ... 1.0]")


###### HyParView
parser.add_argument("--c", type=int, metavar='ACTIVESIZE', default=1,
                    help="c value -> active view Size = log n + c")
parser.add_argument("--k", type=int, metavar='PASSIVESIZE', default=6,
                    help="k value -> passive view Size = activeView size * k")
parser.add_argument("--arwl", type=int, metavar='ARWL', default=6,
                    help="active random walk length -> default 6")
parser.add_argument("--prwl", type=int, metavar='PRWL', default=3,
                    help="passive random walk length -> default 3")
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
stabilizationTime = 49 #args.endtime

## HYPARVIEW variables
c = args.c
k = args.k
ARWL = args.arwl
PRWL = args.prwl
triggerpVMaintain = args.updateViews
maxActiveView = math.ceil(math.log(args.total_nodes,10)) + c
maxPassiveView = math.ceil(math.log(args.total_nodes,10) + c) * k
aVShuffleSize = math.ceil(maxActiveView/2)
pVShuffleSize = math.ceil(maxPassiveView/6)

downNodes = []
upNodes = []

name = "PlumTreeHyParView/PlumTree + HyParView" + str(args.total_nodes)+'-Update'+str(args.updateViews)+'-View'+str(args.c)+'-LOOKAHEAD'+str(args.lookahead) +'-CHURN'+str(args.activeChurn)

simName, startTime, endTime, minDelay, useMPI, mpiLib = name, 0, args.endtime, 0.00001, uMPI, "/usr/lib/x86_64-linux-gnu/libmpich.so"
simianEngine = Simian(simName, startTime, endTime, minDelay, useMPI)

# # Init grid
# positions = []
# # Place nodes in grids
# for x in range(int(math.sqrt(nodes))):
#     for y in range(int(math.sqrt(nodes))):
#         px = 50 + x*60 + random.uniform(-20,20)
#         py = 50 + y*60 + random.uniform(-20,20)
#         positions.append((px,py))


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

class msgHPV:
    def __init__(self,type,newNode,timeToLive,sender):
        self.type = type
        self.newNode = int(newNode)
        self.timeToLive = int(timeToLive)
        self.sender = int(sender)

    def toString(self):
        return '%s-%d-%d-%d'%(self.type,self.newNode,self.timeToLive,self.sender)

class msgHPVShuffle:
    def __init__(self,type,activeView,passiveView,node,timeToLive,sender):
        self.type = type
        self.activeView = eval(activeView)
        self.passiveView = eval(passiveView)
        self.node = int(node)
        self.timeToLive = int(timeToLive)
        self.sender = int(sender)

    def toString(self):
        return '%s-%s-%s-%d-%d-%d'%(self.type,str(self.activeView),str(self.passiveView),self.node,self.timeToLive,self.sender)

class msgReport:
    def __init__(self,type,msgs,degree):
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

        self.report = {}
        self.timersAck = {}

        #hyparview variables
        self.activeView = []
        self.passiveView = []
        self.timerTCP = []
        self.neighborQueue = []

        # USE HYPARVIEW
        #self.GetPeers()
        delay = 10 / self.total_nodes
        delay2 = delay * self.node_idx
      
        if self.node_idx != 0:
            contactNode = 0 #random.randrange(i)
            msg = msgHPV('JOIN',0,0,self.node_idx)
            self.activeView.append(contactNode)
            self.eagerPushPeers.append(contactNode)
            self.reqService(delay2, "HyParView", msg, "Node", contactNode)

        self.reqService(delay2 + 1, "TriggerPassiveViewMaintain", "none")

        self.reqService(endTime - 1, "TriggerSystemReport", "none")

        

    # def GetPeers(self):
    #     for idx in range(self.total_nodes):
    #         if idx!=self.node_idx:
    #             dist = math.sqrt((positions[self.node_idx][0]-positions[idx][0])*
    #                              (positions[self.node_idx][0]-positions[idx][0]) +
    #                              (positions[self.node_idx][1]-positions[idx][1])*
    #                              (positions[self.node_idx][1]-positions[idx][1])
    #                              )
    #             if dist < 100:
    #                 self.eagerPushPeers.append(idx)

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
                if msg.sender not in self.lazyPushPeers and msg.sender in self.activeView:
                    self.lazyPushPeers.append(msg.sender)

            elif msg.type =='IHAVE':
                msgToSend = msgGossip('ACK','','',msg.ID,msg.round,self.node_idx)
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

                if msg.sender not in self.eagerPushPeers and msg.sender in self.activeView:
                    self.eagerPushPeers.append(msg.sender)
                if msg.sender in self.lazyPushPeers:
                    self.lazyPushPeers.remove(msg.sender)
                if msg.ID in self.receivedMsgs.keys():
                    msgToSend = msgGossip('GOSSIP',self.receivedMsgs[msg.ID].payloadType,self.receivedMsgs[msg.ID].payload,msg.ID,msg.round,self.node_idx)
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

                    if msg.sender not in self.eagerPushPeers and msg.sender in self.activeView:
                        self.eagerPushPeers.append(msg.sender)
                    if msg.sender in self.lazyPushPeers:
                        self.lazyPushPeers.remove(msg.sender)
                else:
                    if msg.sender in self.eagerPushPeers:
                        self.eagerPushPeers.remove(msg.sender)
                    if msg.sender not in self.lazyPushPeers and msg.sender in self.activeView:
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
                if m[1] not in self.eagerPushPeers and m[1] in self.activeView:
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

    def HyParView(self, *args):
        msg = args[0]
        #self.out.write(str(self.engine.now) + (":%d rcvd msg '%s' %d\n" % (self.node_idx, msg.type,msg.sender)))
        if self.active:
            if msg.type =='JOIN':
                if len(self.activeView) == maxActiveView:
                    self.dropRandomElementFromActiveView()
                newNode = msg.sender
                self.activeView.append(newNode)
                self.eagerPushPeers.append(newNode)
                for n in self.activeView:
                    if n != newNode:
                        msgToSend = msgHPV('FORWARDJOIN',newNode,ARWL,self.node_idx)
                        self.reqService(lookahead, "HyParView", msgToSend, "Node", n)

            elif msg.type =='FORWARDJOIN':
                if msg.timeToLive == 0 or len(self.activeView) <= 1:
                    self.addNodeActiveView(msg.newNode)
                    msgToSend = msgHPV('NEIGHBORREPLY',self.node_idx,0,self.node_idx)
                    self.reqService(lookahead, "HyParView", msgToSend, "Node", msg.newNode)
                else:
                    if msg.timeToLive == PRWL:
                        self.addNodePassiveView(msg.newNode)

                    available = list(filter(lambda x: x != msg.sender,self.activeView))

                    idx = random.randrange(len(available))
                    n = available[idx]

                    msg.timeToLive -= 1
                    msg.sender = self.node_idx
                    self.reqService(lookahead, "HyParView", msg, "Node", n)


            elif msg.type =='DISCONNECT':
                peer = msg.sender
                if peer in self.activeView:
                    self.activeView.remove(peer)
                    if peer in self.eagerPushPeers:
                        self.eagerPushPeers.remove(peer)
                    elif peer in self.lazyPushPeers:
                        self.lazyPushPeers.remove(peer)

                    #self.NodeFailure(peer)

            elif msg.type =='NEIGHBOR':
                res = 1
                if msg.timeToLive == 0 or len(self.activeView) < maxActiveView:
                    self.addNodeActiveView(msg.newNode)
                    res = 0

                msgToSend = msgHPV('NEIGHBORREPLY',self.node_idx,res,self.node_idx)
                self.reqService(lookahead, "HyParView", msgToSend, "Node", msg.sender)

            elif msg.type =='NEIGHBORREPLY':
                if msg.timeToLive == 0:
                    self.addNodeActiveView(msg.newNode)
                    self.neighborQueue = []
                else:
                    available = list(filter(lambda x: x not in self.neighborQueue,self.passiveView))
                    if len(available) > 0:
                        idx = random.randrange(len(available))
                        n = available[idx]
                        #init timer and try new TCPCONNECT
                        self.timerTCP.append(n)
                        self.reqService(lookahead, "TimerHPV", n)
                        msgToSend = msgHPV('TCPCONNECT',self.node_idx,0,self.node_idx)
                        self.reqService(lookahead / 4, "HyParView", msgToSend, "Node", n)
                    else:
                        self.neighborQueue = []

            elif msg.type =='TCPCONNECT':
                msgToSend = msgHPV('TCPCONNECT_ACK',self.node_idx,0,self.node_idx)
                self.reqService(lookahead / 4, "HyParView", msgToSend, "Node", msg.sender)

            elif msg.type =='TCPCONNECT_ACK':
                if msg.sender in self.timerTCP:
                    self.timerTCP.remove(msg.sender)
                if msg.sender not in self.neighborQueue:
                    self.neighborQueue.append(msg.sender)
                msgToSend = msgHPV('NEIGHBOR',self.node_idx,len(self.activeView),self.node_idx)
                self.reqService(lookahead, "HyParView", msgToSend, "Node", msg.sender)



    def HyParViewShuffle(self, *args):
        if self.active:
            msg = args[0]
            #self.out.write(str(self.engine.now) + (":%d rcvd msg '%s' %s %d\n" % (self.node_idx, msg.type, msg.passiveView,msg.sender)))
            if msg.type =='SHUFFLE':
                if msg.node == msg.sender:
                    ack = msgHPV('SHUFFLE_ACK',self.node_idx,-1,self.node_idx)
                    self.reqService(lookahead, "HyParViewShuffle", ack, "Node", msg.sender)
            
                msg.timeToLive -= 1
                if msg.timeToLive != 0 and len(self.activeView) > 1:
                    #reencaminhar mensagem
                    idx = random.randrange(len(self.activeView))
                    n = self.activeView[idx]
                    if n == msg.sender:
                        if idx == len(self.activeView) - 1:
                            n = self.activeView[idx-1]
                        else:
                            n = self.activeView[idx+1]

                    msg.sender = self.node_idx
                    self.reqService(lookahead, "HyParViewShuffle", msg, "Node", n)
                else:
                    for n in msg.activeView:
                        self.addNodePassiveView(n)

                    for n in msg.passiveView:
                        self.addNodePassiveView(n)
                
                    self.addNodePassiveView(msg.node)
                    size = aVShuffleSize
                    if len(self.activeView) < size:
                        size = len(self.activeView)
                    aVShuffle = random.sample(self.activeView,size)

                    size = pVShuffleSize
                    if len(self.passiveView) < size:
                        size = len(self.passiveView)
                    pVShuffle = random.sample(self.passiveView,size)

                    msgToSend = msgHPVShuffle('SHUFFLEREPLY',str(aVShuffle),str(pVShuffle),msg.node,msg.timeToLive,self.node_idx)
                    #msgToSend = 'SHUFFLEREPLY-%s-%d-%d-%d'%(payload,msg.ID,timeToLive,self.node_idx)
                    self.reqService(lookahead, "HyParViewShuffle", msgToSend, "Node", msg.node)

            elif msg.type =='SHUFFLEREPLY':
                for n in msg.activeView:
                    self.addNodePassiveView(n)

                for n in msg.passiveView:
                    self.addNodePassiveView(n)
            
                self.addNodePassiveView(msg.sender)

                #preencher activeView
                if len(self.activeView) < maxActiveView:
                    self.neighborQueue = []
                    available = list(filter(lambda x: x not in self.neighborQueue,self.passiveView))

                    if len(available) > 0:
                        idx = random.randrange(len(available))
                        n = available[idx]

                        self.timerTCP.append(n)
                        self.reqService(lookahead, "TimerHPV", n)
                        msgToSend = msgHPV('TCPCONNECT',self.node_idx,0,self.node_idx)
                        self.reqService(lookahead / 4, "HyParView", msgToSend, "Node", n)

            elif msg.type =='SHUFFLE_ACK':
                if msg.sender in self.timerTCP:
                    self.timerTCP.remove(msg.sender)
                #if self.engine.now > 50:
               #     self.out.write(str(self.engine.now) + ':ACK\n')


    def dropRandomElementFromActiveView(self,*args):
        idx = random.randrange(len(self.activeView))
        n = self.activeView[idx]
        msgToSend = msgHPV('DISCONNECT',0,0,self.node_idx)
        #msgToSend = 'DISCONNECT--0-0-'+str(self.node_idx)
        self.reqService(lookahead, "HyParView", msgToSend, "Node", n)
        self.activeView.remove(n)
        if n in self.eagerPushPeers:
            self.eagerPushPeers.remove(n)
        elif n in self.lazyPushPeers:
            self.lazyPushPeers.remove(n)
        self.addNodePassiveView(n)

    def addNodeActiveView(self,*args):
        newNode = args[0]
        if newNode != self.node_idx and newNode not in self.activeView:
            if len(self.activeView) == maxActiveView:
                self.dropRandomElementFromActiveView()
            self.activeView.append(newNode)
            self.eagerPushPeers.append(newNode)

            if newNode in self.passiveView:
                self.passiveView.remove(newNode)

    def addNodePassiveView(self,*args):
        newNode = args[0]
        if newNode != self.node_idx and newNode not in self.activeView and newNode not in self.passiveView:
            if len(self.passiveView) == maxPassiveView:
                idx = random.randrange(len(self.passiveView))
                n = self.passiveView[idx]
                self.passiveView.remove(n)
            self.passiveView.append(newNode)

    def NodeFailure(self,node):
        #self.out.write("FAIL DETECTED ---------\n")
        if node in self.activeView:
            self.activeView.remove(node)
            #self.addNodePassiveView(node)
        elif node in self.passiveView:
            self.passiveView.remove(node)

        if node in self.neighborQueue:
            self.neighborQueue.remove(node)

        available = self.passiveView #list(filter(lambda x: x not in self.neighborQueue,self.passiveView))

        if len(available) > 0:
            idx = random.randrange(len(available))
            n = available[idx]

            self.timerTCP.append(n)
            self.reqService(lookahead, "TimerHPV", n)
            msgToSend = msgHPV('TCPCONNECT',self.node_idx,0,self.node_idx)
            self.reqService(lookahead / 4, "HyParView", msgToSend, "Node", n)
        else:
            self.neighborQueue = []

    def TimerHPV(self,*args):
        dest = args[0]
        if dest in self.timerTCP:
            self.timerTCP.remove(dest)
            self.NodeFailure(dest)


#--------------------------------------- TRIGGERS ---------------------------------------------------

    def TriggerPassiveViewMaintain(self, *args):
        if len(self.activeView) > 0 and self.active:
            idx = random.randrange(len(self.activeView))
            n = self.activeView[idx]

            size = aVShuffleSize
            if len(self.activeView) < size:
                size = len(self.activeView)
            aVShuffle = random.sample(self.activeView,size)

            size = pVShuffleSize
            if len(self.passiveView) < size:
                size = len(self.passiveView)
            pVShuffle = random.sample(self.passiveView,size)

            payload = str(aVShuffle) + '|' + str(pVShuffle)
            
            self.timerTCP.append(n)
            self.reqService(lookahead * timerPTacks, "TimerHPV", n)
            msgToSend = msgHPVShuffle('SHUFFLE',str(aVShuffle),str(pVShuffle),self.node_idx,ARWL,self.node_idx)
            self.reqService(lookahead, "HyParViewShuffle", msgToSend, "Node", n)

        if self.engine.now < stabilizationTime:
            self.reqService(triggerpVMaintain, "TriggerPassiveViewMaintain", "none")
        else:
             self.reqService(5, "TriggerPassiveViewMaintain", "none")

    def TriggerSystemReport(self,*args):
        report = []
        degree = len(self.eagerPushPeers)+len(self.lazyPushPeers)
        for m in self.receivedMsgs.keys():
            report.append((m,self.receivedMsgs[m].round,self.report[m][0],self.report[m][1],self.report[m][2]))
        msgToSend = msgReport('reply',report,degree)
        self.reqService(lookahead, "SystemReport", [msgToSend, self.trxMade , self.blockchain.chain, self.miner] , "ReportNode", 0)

    def printViews(self, *args):
        res = ''
        # for m in self.receivedMsgs.keys():
        #     res += self.receivedMsgs[m].toString() + " "
        if self.active:
            self.out.write("%d:Peers %s %s %s msg %s\n"%(self.node_idx,str(self.activeView),str(self.passiveView),str(self.neighborQueue),res))

    def BecomeMiner(self, *args):
        self.miner = True
        print("A am a miner")

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
        idx = random.randrange(len(upNodes))
        n = upNodes[idx]
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

idx = random.randrange(len(upNodes))
n = upNodes[idx]

simianEngine.schedService(50 + lookahead , "create_transaction","" , "Node", n)


simianEngine.run()
simianEngine.exit()