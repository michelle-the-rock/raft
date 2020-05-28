#!/usr/bin/env python

import sys, socket, select, time, json, random

# Your ID number
my_id = sys.argv[1]

# The ID numbers of all the other replicas
replica_ids = sys.argv[2:]

# Connect to the network. All messages to/from other replicas and clients will
# occur over this socket
sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
sock.connect(my_id)

# leader/election related items
# logical clock, starts at 0, increments to 1 when first leader elected
term = 0
# voted_for = the candidate that we have previously cast a ballot for this term
# cleared to FFFF after every election
voted_for = "FFFF"
# leader = current leader if known, else FFFF
leader = "FFFF"
# current number of votes, increments if I'm a candidate, clears to 0 after election
num_votes = 0
# can either be one of: "follower", "candidate", "leader"
current_state = "follower"
# commit_index -> the index of highest log entry committed by this replica  
# initialized to 0, increases monotonically
commit_index = 0

# times!!
# for the last time a message was sent from the reciever
# if leader
time_last_sent = time.time()
# if not the leader
time_last_leader = time.time()
# timeout to resend a message
# set to proper random value in first line of execution, to leader value when leader
timeout = 0

# represents all actions taken
# (key, value, term action logged, client address, message id, have responded to client)
logs = []
# num acks per index for logged messages
num_acks = {}
# next index: for each server, index of the next log entry to send to that server 
# initialized when leader elected to leader last log index + 1 for all followers
next_index = {}
# key-store
data = {}
# queued_requests: the queue of requests that come in during an election - tuples (src, MID)
queued_requests = []

# called by the leader to send the Append RPCs with entries to log and commit index
def send_heartbeat():
	global term
	global my_id
	global sock
	global time_last_sent
	global next_index
	global logs
	global commit_index

	#reset leader timeout
	time_last_sent = time.time()

	for id in replica_ids:
		next_idx = next_index[id]
		entries = []
		# for each entry this follower needs, up to batch size 10
		for index in range(next_idx, min(next_idx + 10, len(logs))):
			log = logs[index]
			# key, value, term, client, MID, have_responded, index
			item = (log[0], log[1], log[2], log[3], log[4], log[5], index)
			entries.append(item)

		# last entry that the leader thinks this follower has
		prev_log_index = next_idx - 1
		if len(logs) == 0:
			prev_log_term = term
		else:
			prev_log_term = logs[prev_log_index][2]
		#if log is empty, then prev_log_index is -1 and term is current term

		packet = {
			"src": my_id,
			"dst": id,
			"leader": my_id,
			"term": term,
			"type": "append",
			"leaderCommit": commit_index,
			"prevLogIndex": prev_log_index,
			"prevLogTerm": prev_log_term,
			"entries": entries,
		}

		packet_str = json.dumps(packet)
		sock.sendto(packet_str, id)

# empty the requests queued during an election/when leader wasn't known
def send_queued_requests():
	global queued_requests
	global my_id
	global leader
	global sock

	for (client, MID) in queued_requests:
		redirect_msg = {"src": my_id, "dst": client, "leader": leader, "type": "redirect", "MID": MID}
		redirect_msg_str = json.dumps(redirect_msg)
		sock.sendto(redirect_msg_str, client)
	
	#empty the queued requests, we have sent them along to the leader
	queued_requests = []

# Term has changed, I need to update my state
def update_state_after_election(packet):
	global term
	global leader
	global voted_for
	global current_state
	global num_votes

	leader_term = packet["term"]

	# if I was a follower or candidate, and this leader has a new term, I now follow them
	if current_state == "follower" and leader_term >= term:
		term = leader_term
		leader = packet["leader"]
		voted_for = "FFFF"

	elif current_state == "candidate" and leader_term >= term:
		term = leader_term
		leader = packet["leader"]
		current_state = "follower"
		voted_for = "FFFF"
		num_votes = 0

	# should never be equal, so greater than is fine, I'm a follower now
	elif current_state == "leader" and leader_term > term:
		current_state = "follower"
		term = leader_term
		leader = packet["leader"]

	# Now that I know the leader (double checking here), send them the requests I queued
	if leader != "FFFF":
		send_queued_requests()

# send a False ack to a heartbeat
def reply_false(packet):
	global my_id
	global term
	global commit_index
	global sock
	global logs

	response = {
		"src": my_id,
		"dst": packet["src"],
		"type": "ack",
		"leader": packet["leader"],
		"success": False,
		"term": term,
		"commitIndex": commit_index,
		"currLogIndex": len(logs) - 1,
	}
	response_str = json.dumps(response)
	sock.sendto(response_str, packet["src"])

# Handle heartbeat as a candidate or follower
def handle_heartbeat(packet):
	global term
	global leader
	global time_last_leader
	global data
	global logs
	global commit_index

	# update timeout, I have just heard from a leader
	time_last_leader = time.time()

	leader_term = packet["term"]

	# If the leader is different or the term is different, I need to catch up
	if packet["leader"] != leader or leader_term > term:
		update_state_after_election(packet)

	# If leader term is out of date, tell them
	if leader_term < term:
		reply_false(packet)
		return

	leader_commit = packet["leaderCommit"]
	prev_log_index = packet["prevLogIndex"]
	prev_log_term = packet["prevLogTerm"]

	#is this entry (the last thing the leader thinks we have) in our log
	if len(logs) > prev_log_index:
		#do we disagree on what that last entry is, then fail
		if prev_log_index > -1 and logs[prev_log_index][2] != prev_log_term:
			reply_false(packet)

		# append all new entries to the log
		for entry in packet["entries"]:
			# key, value, term, client address, message id, have responded, index
			entry_index = entry[6]
			# if this entry is to be added at the end of our log
			if entry_index == len(logs):
				logs.append((entry[0], entry[1], entry[2], entry[3], entry[4], entry[5]))
			# if you've sent me something I already have and the terms are different, then fail, else do nothing
			elif entry_index < len(logs) and entry[2] != logs[entry_index][2]:
				del logs[entry_index:]
				# don't add any more after deleting, let the leader catch us up
				break

		# if the leader has more things commited, I catch up
		if leader_commit > commit_index:
			# commit everything up until the index of the last new entry
			temp_commit_index = min(leader_commit, len(logs) - 1)
			for index in range(commit_index, temp_commit_index + 1):
				log = logs[index]
				key = log[0]
				value = log[1]
				data[key] = value
			commit_index = temp_commit_index

		response = {
			"src": my_id,
			"dst": packet["src"],
			"type": "ack",
			"leader": packet["leader"],
			"success": True,
			"term": term,
			"currLogIndex": len(logs) - 1,
			"commitIndex": commit_index
		}
		response_str = json.dumps(response)
		sock.sendto(response_str, packet["src"])
	else:
		# I don't have the last thing the leader thinks I do, can't add these entries
		reply_false(packet)

	# refresh timer to minimize timeouts
	time_last_leader = time.time()

# leader handle acks to its heartbeats
def handle_ack(packet):
	global data
	global next_index
	global commit_index
	global current_state
	global term
	global leader

	# ignore, am no longer thse leader
	if current_state != "leader":
		return

	src = packet["src"]
	# packet from a future term, update and become follower
	if packet["term"] > term:
		term = packet["term"]
		current_state = "follower"
		# don't know the leader at the moment, it's just not me
		leader = "FFFF"
		return

	if not packet["success"]:
		# decrement next index to send to the last thing they committed, since it shouldn't add duplicates, this is safe
		next_index[src] = packet["commitIndex"]
		return

	if packet["term"] < term:
		# got a stale request - doing nothing
		return

	# if success = true
	curr_log_index = packet["currLogIndex"]

	# the election invariant should disallow this (5.4.1)
	if curr_log_index >= len(logs):
		return

	# old next index
	next_idx = next_index[src]
	# from what we last sent to this replica, to the length of its log
	for index in range(next_idx, curr_log_index + 1):
		num_acks[index] += 1
		log = logs[index]
		# check if we got quorum (the first time, since have responded to client (log[5]) is False
		if num_acks[index] > len(replica_ids) / 2 and not log[5]:
			key = log[0]
			value = log[1]
			data[key] = value
			commit_index = index
			send_response_to_client(log, index)

	next_index[src] = curr_log_index + 1

# we have just committed their entry, sent ok to client
def send_response_to_client(log, index):
	global logs
	global my_id
	global sock

	# another check, if we (this leader, at least) haven't already oked this request
	if not log[5]:
		logs[index] = (log[0], log[1], log[2], log[3], log[4], True)
		response = {
			"src": my_id,
			"dst": log[3],
			"MID": log[4],
			"type": "ok",
			"leader": my_id
		}

		response_str = json.dumps(response)
		sock.sendto(response_str, log[3])

def send_redirect(packet):
	global queued_requests

	# if leader is FFFF, which is updated to be the case during elections (since we haven't confirmed yet who the new leader is)
	if leader != "FFFF":
		redirect_msg = {"src": my_id, "dst": packet["src"], "leader": leader, "type": "redirect", "MID": packet["MID"]}
		redirect_msg_str = json.dumps(redirect_msg)
		sock.sendto(redirect_msg_str, packet["src"])
	else:
		queued_requests.append((packet["src"], packet["MID"]))

# update my state to be candidate and request votes
def become_candidate():
	global term
	global my_id
	global sock
	global num_votes
	global current_state
	global time_last_leader 
	global leader
	global voted_for

	current_state = "candidate"
	term += 1
	num_votes = 1
	voted_for = my_id

	last_index = len(logs) - 1
	# first term edge case
	if last_index == -1:
		last_term = 0
	else:
		last_term = logs[last_index][2]

	#leader is FFFF since we don't know who the leader is yet for this new term
	packet = {'src': my_id, 'dst': "FFFF", 'leader': "FFFF", 'term': term, 'type': "request vote", "lastIndex": last_index, "lastTerm": last_term}
	packet_str = json.dumps(packet)
	sock.sendto(packet_str, "FFFF")
	# candidates mind this timeout field, so we refeesh the value to candidates don't timeout
	time_last_leader = time.time()

# send vote messages, updating state/term to match
def send_vote(packet):
	global term
	global current_state
	global time_last_leader
	global voted_for

	current_state = "follower"
	term = packet["term"]
	time_last_leader = time.time()
	voted_for = packet["src"]

	# take the leader from the packet, which is FFFF, since we still don't know the leader yet mid-election
	response = {'src': my_id, 'dst': packet['src'], 'leader': packet['leader'], 'term': term, "type": "vote"}
	response_str = json.dumps(response)
	sock.sendto(response_str, packet["src"])
	time_last_leader = time.time()

# replicas handle the request vote type messages, based on their states and terms
def handle_request_vote(packet):
	global term
	global current_state
	global voted_for
	global logs
	global time_last_leader
	global leader

	# we have recieved contact from someone trying to be leader, let's hold off on timing out for a bit
	# time_last_leader = time.time()

	# leaders and followers from past terms who haven't voted yet
	if current_state != "candidate" and packet["term"] > term and voted_for == "FFFF":
		#if you recieve this message and you are not a candidate, you are automatically a follower now (regardless of what you were before)
		# we no longer know who the leader is if we're voting, so
		leader = "FFFF"

		#first, I need to not have voted for anyone else this term
		#if -1, then your log empty, I can vote for you
		# or if your log up to date, I can vote for you
		my_last_index = len(logs) - 1

		if packet["lastIndex"] == -1 or my_last_index == -1:
			# there have been no logs
			send_vote(packet)
			return

		my_last_term = logs[my_last_index][2]

		# this candidate's last term value committed is better than mine or the same and their log is longer
		# this candidate is at least as up to date as I am
		if packet["lastTerm"] > my_last_term or (packet["lastTerm"] == my_last_term and packet["lastIndex"] >= my_last_index):
			send_vote(packet)

	# if this candiate is from the future, I am follower now, but don't know the leader
	if current_state == "candidate" and packet["term"] > term:
		leader = "FFFF"
		current_state = "follower"
		term = packet["term"]

		# first, I need to not have voted for anyone else this term
		# if -1, then your log empty, I can vote for you
		# or if your log up to date, I can vote for you
		my_last_index = len(logs) - 1

		if packet["lastIndex"] == -1 or my_last_index == -1:
			# there have been no logs
			send_vote(packet)
			return

		my_last_term = logs[my_last_index][2]

		# same case as above
		if packet["lastTerm"] > my_last_term or (packet["lastTerm"] == my_last_term and packet["lastIndex"] >= my_last_index):
			send_vote(packet)

# replica handle packets of type vote, based on state and term
def handle_vote(packet):
	global current_state
	global term
	global num_votes
	global replica_ids
	global time_last_leader
	global leader
	global my_id

	if current_state == "candidate" and packet["term"] == term:
		num_votes += 1
		time_last_leader = time.time()
		if num_votes > len(replica_ids) / 2 and leader != my_id:
			become_leader()

# update my state to leader
def become_leader():
	global current_state
	global term
	global num_votes
	global replica_ids
	global leader
	global voted_for
	global time_last_leader
	global next_index
	global data
	global num_acks

	# resets the data for the leader
	current_state = "leader"
	num_votes = 0
	voted_for = "FFFF"
	leader = my_id

	# reinitialize next index
	for id in replica_ids:
		next_index[id] = len(logs)

	# commit everything that you have
	for index in range(commit_index, len(logs)):
		log = logs[index]
		key = log[0]
		value = log[1]
		data[key] = value
		# will duplicate put responses (sometimes), best performance when commented out
		# send_response_to_client(log, index)

	# initialize for this leader to avoid key errors
	for index in range(0, len(logs)):
		num_acks[index] = 1
	
	#send first heartbeat to alert followers to my leadership
	send_heartbeat()
	# now that an election is over, handle all of the queued requests that have been accumulated
	# this will send redirects to clients with me as the leader, which is fine
	send_queued_requests()

# set timeout duration based on state, randomness
def set_timeout():
	global timeout

	if current_state == "leader":
		timeout = 0.1
	else:
		timeout = random.randint(350, 701) * 0.001

# leader handles this get request by returning the data
def leader_handle_get(packet):
	global data
	global my_id

	key = packet['key']

	if key in data:
		value = data[key]
	else:
		value = ""
	#used for debugging
	#logs_with_key = filter(lambda log: log[0] == packet["key"], logs)
	# send the response message
	response = {'src': my_id, 'dst': packet['src'], 'leader': my_id, 'type': "ok", 'MID': packet['MID'],
				'value': value}
	response_str = json.dumps(response)
	sock.sendto(response_str, packet['src'])

# leader handle put by logging and prepping to tell replicas
def leader_handle_put(packet):
	global logs
	global num_acks
	global term

	key = packet['key']
	value = packet['value']

	# index = position in list, action, key/value, term, client address, message id, have we responded to client yet
	logs.append((key, value, term, packet["src"], packet["MID"], False))
	index = len(logs) - 1
	num_acks[index] = 1

#END HELPER FUNCTIONS

# main loop of replicas
set_timeout()
while True:		
	ready = select.select([sock], [], [], 0.01)[0]
	
	if sock in ready:
		msg_raw = sock.recv(32768)
		
		if len(msg_raw) == 0: continue
		msg = json.loads(msg_raw)

		if msg['type'] == 'get':
			print("MID(get): " + msg["MID"])
			if current_state != "leader":
				send_redirect(msg)
			else:	
				leader_handle_get(msg)
			
		elif msg['type'] == 'put':
			print("MID(put): " + msg["MID"])
			if current_state != "leader":
				send_redirect(msg)
			else:
				leader_handle_put(msg)
		
		# Handle noop messages. This may be removed from your final implementation
		# elif msg['type'] == 'noop':
			# print '%s received a NOOP from %s' % (msg['dst'], msg['src'])

		elif msg['type'] == 'request vote':
			handle_request_vote(msg)

		elif msg["type"] == "vote": 
			handle_vote(msg)

		elif msg["type"] == "append":
			handle_heartbeat(msg)

		elif msg["type"] == "ack":
			handle_ack(msg)

	current_time = time.time()
	if current_state == "leader":
		diff =  current_time - time_last_sent
		if diff >= timeout:
			send_heartbeat()
			set_timeout()	
	else:
		diff = current_time - time_last_leader
		if diff >= timeout:	
			become_candidate()
			set_timeout()
