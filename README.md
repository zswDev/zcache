
# this is L3 cache, lmdb=>redis=>mongodb
## get qps is fast

### sharedMemory => remoteMemory => remoteDisk
### 1%           => 20%          => 80%
### 100us        => 1ms          => 10ms


# TOTO
### register     => heap         => local 
### 0.001%       => 0.01%        => 0.1 % 
### 1ns          => 1us          => 10us