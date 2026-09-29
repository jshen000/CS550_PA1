CS550 PA#1 - Napster-style P2P file sharing
User manual and submission notes | September 28, 2026

1. STATUS AND REQUIREMENTS
This package documents the supplied implementation and recorded experiments.
It is not a claim that all assignment requirements are complete. Read
SUBMISSION_CHECKLIST.txt before submitting. In particular, the medium study
uses 800 files, not the required 1,000; the third concurrent large-file trial
failed; the complete hourly development history and all AI prompts still need
the student's records. The report is pa1-report.pdf.

2. REQUIREMENTS AND INSTALLATION
Evaluation target in the assignment: Ubuntu 26.04 LTS, three VMs: one index
and two peers. Python 3 is required (the code uses modern pathlib features;
use the Ubuntu-provided Python 3). No third-party Python packages are needed
for the application. GNU make is used for syntax compilation and launch targets.
Install if needed on Ubuntu: sudo apt install python3 make openssh-client
Run all commands below from the extracted source directory. Peer shared paths
are resolved relative to the JSON configuration file, not the shell directory.
Run: make
This compiles Python source to bytecode for a syntax check; it does not perform
three-VM testing. The report plots were generated separately during documentation.

3. FILE ORGANIZATION
server.py               Central TCP index, exact filename search, peer registry.
peer_server.py          Threaded obtain and replica-download service.
register.py             Batched registration and replication coordinator.
search.py               Exact-match lookup; displays peer IDs and endpoints.
download.py             Search, source selection, binary download, new-owner registration.
config.py               Peer JSON configuration parsing and validation.
generate_dataset.py     Generates up to one million 1 KiB ASCII-text files.
benchmark_search.py     Search timing with raw CSVs, metadata, and repeat summaries.
benchmark_transfer.py   1 KiB search+transfer timing; not a medium/large benchmark.
run_all_tests.py         SSH-coordinated full experiments and small-file mode.
auto_node.py             Worker used by the full experiment coordinator.
run_staged_tests.py      Reduced 800 x 1 MiB and 8 x 1 GiB staged experiments.
worker_staged_*.py       Preserved generated workers for recorded staged runs.
config.json             Original VM1 settings (edit for your deployment).
config_*.example.json   Separate example settings for the two peers.
suite.json              SSH experiment coordinator settings (verify paths).
index_config.json       Historical configuration; server.py DOES NOT read it.
Makefile                Syntax check and convenience launch targets.
results/                Supplied experiment evidence, including failed/partial runs.
figures/                Derived performance plot.
docs/                   Design, evidence, original runner guide, and calculations.
output.txt              Actual local functional verification plus recorded VM log excerpt.
development_log.txt     Evidence-based reconstruction and missing-history instructions.
readme.txt              This manual.
pa1-report.pdf          Report with methods, tables, figures, analysis and limitations.
Legacy utilities client.py, lookup_download.py, list_files.py and the misspelled
 generate_dateset.py are retained for provenance. Use the main commands below.
No benchmark datasets or secret SSH keys are included. Generate data on the VMs.

4. DEPLOYMENT AND CONFIGURATION
Recorded endpoints: index/VM3 10.0.2.6:9000; peer_a/VM1 10.0.2.5:9001;
peer_b/VM2 10.0.2.15:9002. Replace these with your current reachable VM addresses.
Use each VM's local filesystem; no NFS/shared filesystem is required.
TCP 9000 must be reachable from peers; each peer service port must be reachable
from the other peer. SSH port 22 on VM2 is used only for test coordination.
The listen_host 0.0.0.0 binds all interfaces; peer.host is the advertised,
reachable address and must not be 0.0.0.0.

VM1:
  cp config_peer_a.example.json config.json
  mkdir -p peer_a/shared
VM2:
  cp config_peer_b.example.json config.json
  mkdir -p peer_b/shared
Edit config.json on each machine before starting. Keep IDs unique.

5. START SERVICES (THREE SEPARATE TERMINALS/VMs)
VM3 index, baseline factor 1:
  python3 server.py --host 0.0.0.0 --port 9000 --replication-factor 1
Equivalent: make index REPLICATION_FACTOR=1
Do NOT pass --config to server.py; that option is not implemented.
VM1 peer:
  python3 peer_server.py --config config.json
VM2 peer:
  python3 peer_server.py --config config.json
Equivalent on each peer: make peer
Keep all three services running. Use additional terminals for commands below.

6. REGISTER, SEARCH, DOWNLOAD
On VM1, create a small demo file and register it:
  printf 'Hello from peer_a\n' > peer_a/shared/demo_a.txt
  python3 register.py --config config.json
On VM2:
  python3 register.py --config config.json
  python3 search.py --config config.json --file demo_a.txt
  python3 download.py --config config.json --file demo_a.txt --source-peer peer_a
The search prints matching peer IDs, hosts and ports. By default it excludes
the requesting peer. --all-owners includes it. download.py chooses a named
source with --source-peer, or tries returned sources in order when omitted.
A successful download prints: display file 'demo_a.txt'
It does not print file contents. The received file goes into the shared directory,
replaces a same-name file atomically, and is registered as owned by the downloader.
Compare sha256sum of demo_a.txt on both VMs to verify identical contents.
Text and binary files use the same byte-oriented transfer path.
Interactive alternatives: omit --file for a filename prompt.

7. REPLICATION DEMONSTRATION (USE A SMALL SEPARATE DATASET)
Replication factor means total registered copies, including the original.
Start VM3 with --replication-factor 2. Both peer services must be running.
Register both peers first, even if one has an empty shared directory:
  python3 register.py --config config.json --no-replicate
Run this on each peer. Then run on the source peer:
  python3 register.py --config config.json
Check both owners:
  python3 search.py --config config.json --file demo_a.txt --all-owners
Verify the replica bytes with sha256sum. At most two copies are possible with
two peers. A factor above available peers is reported as UNDER-REPLICATED.
--no-replicate bypasses replica creation; it must not be used as evidence that
the requested replication factor is satisfied. Replication is not continuous
repair and does not detect later peer failure or deleted files automatically.

8. RESTART PROCEDURE
After an index restart, its in-memory metadata is empty. Restart the index,
start both peer servers if necessary, then register the surviving shared files
on BOTH peers. After staged cleanup, restart the index before re-registering:
registration only adds records and does not remove records for deleted files.
For baseline timing use factor 1 explicitly, and document that configuration.
The supplied staged environment records factor 2 even though the runner bypassed
replication; those runs are not evidence of factor-2 dataset resilience.

9. SEARCH BENCHMARK EXAMPLES
Create/register the required data on each peer before timing. Generating a
million small files needs many inodes and substantially more allocated disk
space than their logical sizes suggest.
  python3 generate_dataset.py --config config.json --count 1000000
  python3 register.py --config config.json --no-replicate
One requester (VM1, both peer services still running):
  python3 benchmark_search.py --config config.json --target-peer peer_b --requests 10000 --dataset-count 1000000 --repeats 3 --label search_1peer
Two requesters: use --repeats 1 and --start-at with the SAME future Unix timestamp
on both VMs, one command targeting peer_b and the other targeting peer_a.
Repeat three times with fresh future timestamps and trial labels. Synchronize
VM clocks first; calculate total completion time from earliest start to latest
finish across both requesters. Each requester performs 10,000 requests.

10. TRANSFER BENCHMARKS AND AUTOMATION
For 10,000 small-file transfers by one requester on VM1:
  python3 benchmark_transfer.py --config config.json --target-peer peer_b --files 10000 --repeats 3 --label transfer_1peer
For two requesters, each transfers 5,000 files concurrently, with --repeats 1
and a common future --start-at timestamp. Reverse the target on VM2.
Use run_all_tests.py for coordinated experiments. First edit suite.json to
match actual VM directories, SSH user/address and key. Original suite.json
uses /home/vboxuser/CS550/p2p, while later evidence uses
/home/vboxuser/data/CS550/p2p. Do not assume either is currently correct.
Verify key login and host identity before unattended runs:
  ssh -i ~/.ssh/cs550_test -o BatchMode=yes vboxuser@10.0.2.15 hostname
The private key stays on VM1 and is not part of the submission.
Check full-suite storage first:
  python3 run_all_tests.py --suite suite.json --check-only
Read docs/experiment_runner_original.txt before generating the full dataset.
Supply --host-free-gib only with actual measured free space on the host volume
holding the VM images. A larger nominal VM disk does not create host space.
Full run after capacity and settings are verified:
  python3 run_all_tests.py --suite suite.json --host-free-gib ACTUAL_FREE_GIB
ACTUAL_FREE_GIB is a placeholder to replace with the measured numeric value.
Small concurrent-transfer trials only:
  python3 run_all_tests.py --suite suite.json --only-small
Reduced staged run, if intentionally accepting reduced coverage:
  python3 run_staged_tests.py --suite suite.json
This last command creates 800 medium and 8 large sources per peer in stages,
verifies received SHA-256, deletes received copies, and cleans its generated
stage sources. It does NOT meet the full simultaneous-dataset requirement.
Do not launch overlapping runners. Preserve failed logs as failed evidence.

11. TROUBLESHOOTING / KNOWN LIMITATIONS
Connection refused: check index/peer processes, address and port.
No route to host / timed out: check VM power/network state, routing and firewall;
the latest large-file logs contain these errors. Root cause is not established.
No matching source: confirm spelling, ownership registration and index restart.
Under-replicated: start/register enough peers and retry source registration.
Index has stale entries: restart it and re-register existing files on both peers.
Insufficient capacity: free actual host and guest space; never falsify capacity.
Index metadata is not persistent; the index is a single point of failure.
Each accepted connection creates a thread; there is no bounded worker pool.
A single index lock serializes shared-dictionary operations.
Transfers use temporary files and atomic replacement; there is no fsync durability
promise, transfer resume, authentication or encryption in the application.
Regular CLI transfers do not hash file contents; staged tests add SHA-256 checks.
The application assumes stable registered contents, as the assignment permits.

12. SUBMISSION
Submit one compressed package through Canvas and the code to the required private
git repository. Verify the current deadline in Canvas: the assignment document
has a Fall 2026 heading but inconsistent 2023 date lines.
Complete development_log.txt with actual hourly activity and all AI prompts;
add actual VM screenshots, finish or disclose remaining experiments, and review
the report before submitting. Documentation does not repair missing evidence.
