CS550 PA1 - Simple P2P File Sharing

1. Requirements
Python 3 and three Ubuntu VMs. No extra Python packages are needed.
Run commands from your project folder, for example:
  cd ~/CS550/p2p
If you moved the project, use ~/data/CS550/p2p instead.

2. Configuration
Check config.json on each peer before starting:
  VM1: peer_a, host 10.0.2.5, port 9001, shared_dir peer_a/shared
  VM2: peer_b, host 10.0.2.15, port 9002, shared_dir peer_b/shared
Both peers use index host 10.0.2.6 and index port 9000.
Use listen_host 0.0.0.0. Update the IP addresses if they changed.

3. Start the services
Start VM3 first:
  python3 server.py --host 0.0.0.0 --port 9000 --replication-factor 1
Start Peer A on VM1:
  python3 peer_server.py --config config.json
Start Peer B on VM2:
  python3 peer_server.py --config config.json
Keep all three terminals open. Use new terminals for the following commands.

4. Register files
Put files in each peer's shared directory. On BOTH VM1 and VM2, run:
  python3 register.py --config config.json --no-replicate
Repeat registration on both peers after restarting the index server.

5. Search and download
Example: put hello.txt in peer_a/shared on VM1 and register it.
On VM2, search:
  python3 search.py --config config.json --file hello.txt
On VM2, download:
  python3 download.py --config config.json --file hello.txt --source-peer peer_a
The file is saved in peer_b/shared. The program prints:
  display file 'hello.txt'

6. Replication (optional demonstration)
Restart the index with --replication-factor 2 instead of 1.
Register BOTH peers first using --no-replicate as in step 4.
Then run on VM1 and VM2, one at a time:
  python3 register.py --config config.json
This can copy all shared files. Use a small dataset or ensure enough disk space.
Factor 2 means two total copies, including the original.

7. Main files
server.py         Central index server
peer_server.py    Peer file server
register.py       Register files and create replicas
search.py         Search for a file
download.py      Download a file
config.json       Peer settings
results/          Recorded experiment results
pa1-report.pdf    Project report

8. Notes
The index is stored in memory and is cleared on restart.
Current server.py uses command-line options, not index_config.json.
See pa1-report.pdf for test results and incomplete evaluation requirements.
