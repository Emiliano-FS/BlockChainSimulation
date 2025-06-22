from hashlib import sha256
import random
import json
import time


class Transaction:
    def __init__(self, author):
        self.trans_id = "T-" +str(random.randint(11111111,99999999))
        self.author = author
        self.timestamp = time.time()
        self.content = "Transaccion de " + str(self.author) + " con numero " + self.trans_id +" con tiempo "+ str(self.timestamp)

    def __str__(self):
        return f"Transaction(trans_id={self.trans_id}, author={self.author}, timestamp={self.timestamp} , content={self.content})"
    
    def to_dict(self):
        return {
            "trans_id": self.trans_id,
            "author": self.author,
            "timestamp": self.timestamp,
            "content": self.content
        }

    @classmethod
    def from_dict(cls, data):
        tx = cls(data["author"])
        tx.trans_id = data["trans_id"]
        tx.timestamp = data["timestamp"]
        tx.content = data["content"]
        return tx

class Block:
    def __init__(self, index, transactions, timestamp, previous_hash, nonce=0):
        self.index = index
        self.transactions = transactions
        self.timestamp = timestamp
        self.previous_hash = previous_hash
        self.nonce = nonce

    def to_dict(self):
        return {
            "index": self.index,
            "transactions": [tx.__dict__ for tx in self.transactions],
            "timestamp": self.timestamp,
            "previous_hash": self.previous_hash,
            "nonce": self.nonce,
            "hash": getattr(self, "hash", None)
        }

    @classmethod
    def from_dict(cls, data):
        block = cls(
            index=data["index"],
            transactions=[Transaction.from_dict(tx) for tx in data["transactions"]],
            timestamp=data["timestamp"],
            previous_hash=data["previous_hash"],
            nonce=data["nonce"]
        )
        block.hash = data.get("hash")
        return block    

    def compute_hash(self):
        block_content = {
            "index": self.index,
            "transactions": [tx.__dict__ for tx in self.transactions],
            "timestamp": self.timestamp,
            "previous_hash": self.previous_hash,
            "nonce": self.nonce
        }
        block_string = json.dumps(block_content, sort_keys=True)
        return sha256(block_string.encode()).hexdigest()
    
    def __str__(self):
        return f"BLOCK (ID = {self.index}, TRX={str(len(self.transactions))}, timestamp={self.timestamp} , PREVIHASH={self.previous_hash})"
    
    def __repr__(self):
        return self.__str__()


class Blockchain:
    # difficulty of our PoW algorithm
    difficulty = 1

    def __init__(self):
        self.unconfirmed_transactions = []
        self.chain = []
        self.forks = {}
        self.orphans = []

    def create_genesis_block(self):
        """
        A function to generate genesis block and appends it to
        the chain. The block has index 0, previous_hash as 0, and
        a valid hash.
        """
        genesis_block = Block(0, [], 0, "0")
        genesis_block.hash = genesis_block.compute_hash()
        self.chain.append(genesis_block)

    @property
    def last_block(self):
        return self.chain[-1]
    
    def block_validity(self, block, proof):

        if not Blockchain.is_valid_proof(block, proof):
            print("It failed the Proof ?????  ")
            return False
        
        return True


    def add_block(self, block):
        """
        A function that adds the block to the chain after verification.
        Verification includes:
        * Checking if the proof is valid.
        * The previous_hash referred in the block and the hash of latest block
          in the chain match.
        """
        
        self.chain.append(block)
        #print("I added the block")
        return True
            

    @staticmethod
    def proof_of_work(block):
        """
        Function that tries different values of nonce to get a hash
        that satisfies our difficulty criteria.
        """
        block.nonce = 0

        computed_hash = block.compute_hash()
        print("Intentando hacer el Proof of work para " + str(block.index))
        #print(computed_hash)
        while not computed_hash.startswith('0' * Blockchain.difficulty):
            block.nonce += 1
            computed_hash = block.compute_hash()

        print("Lo encontre despues de " + str(block.nonce))
        return computed_hash

    def add_new_transaction(self, transaction):
        self.unconfirmed_transactions.append(transaction)

    @classmethod
    def is_valid_proof(cls, block, block_hash):
        """
        Check if block_hash is valid hash of block and satisfies
        the difficulty criteria.
        """
        return (block_hash.startswith('0' * Blockchain.difficulty) and
                block_hash == block.compute_hash())


    @classmethod
    def check_chain_validity(cls, chain):
        result = True
        previous_hash = "0"
    
        for block in chain:
            # Reconstruct block to recompute its hash
            reconstructed = Block(
                index=block.index,
                transactions=block.transactions,
                timestamp=block.timestamp,
                previous_hash=block.previous_hash,
                nonce=block.nonce
            )
            computed_hash = reconstructed.compute_hash()
    
            # Explicit comparison
            if computed_hash != block.hash or previous_hash != block.previous_hash:
                print(f"[INVALID BLOCK] index={block.index}, expected_hash={computed_hash}, got={block.hash}")
                result = False
                break

            previous_hash = block.hash
    
        return result

    def mine(self):
        """
        This function serves as an interface to add the pending
        transactions to the blockchain by adding them to the block
        and figuring out Proof Of Work.
        """
        if not self.unconfirmed_transactions:
            return False

        last_block = self.last_block

        new_block = Block(index=last_block.index + 1,
                          transactions=self.unconfirmed_transactions[:100],
                          timestamp=time.time(),
                          previous_hash=last_block.hash)
        
        print("Cree el bloque "+ str(new_block.index))

        proof = self.proof_of_work(new_block)

        if Blockchain.is_valid_proof(new_block, proof):
            new_block.hash = proof
            self.add_block(new_block)
            self.unconfirmed_transactions = self.unconfirmed_transactions[100:]
            return True

        return False

    def consensus(self, block):
        # Step 1: Validate block hash and PoW
        if not self.block_validity(block, block.hash):
            print(f"❌ Invalid block {block.index}")
            return False

        # Step 2: Normal chain extension
        if block.previous_hash == self.last_block.hash:
            self.add_block(block)
            self.resolve_forks()
            self.remove_if_orphan(block)
            self.try_attach_orphans()
            return True

        # Step 3: Extends an existing fork
        elif block.previous_hash in self.forks:
            print(f"🔀 Block {block.index} extends known fork")
            self.forks[block.previous_hash][0].append(block)
            self.forks[block.hash] = self.forks.pop(block.previous_hash)
            self.resolve_forks()
            self.remove_if_orphan(block)
            self.try_attach_orphans()
            return True

        # Step 4: Fork from main chain (fork base exists in chain)
        elif block.previous_hash in self.chain_hashes():
            base_index = self.chain_index(block.previous_hash)
            self.forks[block.hash] = ([block], base_index)
            print(f"🌱 New fork started at block {block.index}")
            self.remove_if_orphan(block)
            self.try_attach_orphans()
            return True

        # Step 5: Orphan block (no parent known yet)
        else:
            print(f"🧩 Orphan block received: {block.index}")
            if block not in self.orphans:
                self.orphans.append(block)
            return False


    def resolve_forks(self):
        longest_chain = self.chain
        for fork_blocks, base_index in self.forks.values():
            candidate_chain = self.chain[:base_index + 1] + fork_blocks
            if len(candidate_chain) > len(longest_chain):
                longest_chain = candidate_chain

        if len(longest_chain) > len(self.chain) + 1:
            self.chain = longest_chain
            print("Fork resolved — replaced with longer fork")


    def remove_if_orphan(self, block):
        """Safely remove a block from orphans if it's there."""
        try:
            self.orphans.remove(block)
        except ValueError:
            pass

    def try_attach_orphans(self):
        """Try attaching all known orphans to current chain or forks."""
        reattachable = []
        for orphan in self.orphans:
            if self.consensus(orphan):
                reattachable.append(orphan)
    
        # Remove attached orphans
        if reattachable:
            print(f"🔁 Reattached {len(reattachable)} orphan(s)")
            self.orphans = [o for o in self.orphans if o not in reattachable]


    def chain_hashes(self):
        return {block.hash for block in self.chain}

    def chain_index(self, hash_value):
        for i, block in enumerate(self.chain):
            if block.hash == hash_value:
                return i
        return 0

    
    def remove_confirmed_transactions(self, block):
        self.unconfirmed_transactions = [
            tx for tx in self.unconfirmed_transactions
            if tx.trans_id not in {t.trans_id for t in block.transactions}
        ]