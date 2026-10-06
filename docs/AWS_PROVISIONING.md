# AWS provisioning

A 1 GB EC2 instance (`t3.micro`, `t4g.micro`) with no swap runs Synthigy, but the
engine is its biggest process, about 500 MB. When anything else on the box asks
for memory that is not there — apt's daily package refresh, a busy proxy — the
kernel's OOM killer kills the engine. Swap, one less memory spike and a service
that relaunches the engine keep the box up.

Step 3 needs CLI v0.1.11 or newer (`synthigy version`; `synthigy upgrade`).

```bash
# 0. check: swap should show 0
free -h

# 1. 2 GB swap
sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
echo 'vm.swappiness=10' | sudo tee /etc/sysctl.d/99-swap.conf && sudo sysctl --system

# 2. remove the apt hook that ran appstreamcli (not needed on a server)
apt-get -s purge appstream        # dry run: should remove only appstream
sudo apt-get purge -y appstream

# 3. run Synthigy as a service so it starts at boot and outlives logout
cd ~/myapp                        # the folder that holds .synthigy/
synthigy down
synthigy service install
sudo loginctl enable-linger $USER
synthigy service status
```

## Checking it

- `free -h` shows 2.0Gi of swap.
- `synthigy service status` shows `active (running)`.
- When the engine is killed, the portal relaunches it and says so in the journal:

```bash
journalctl -k | grep -i oom            # what the kernel killed, and when
journalctl --user -u synthigy -f       # Engine died: killed by SIGKILL, ... — relaunching.
```

An engine that was up is relaunched each time it dies. One that dies while
starting is not: the portal keeps running and shows why (`synthigy logs`).

See [Running Synthigy](OPERATIONS.md) for services in general.
