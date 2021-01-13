# Tencent Block Traces Exploration

## Converting to 4k 
The current traces are present as offset values. We need to convert them to 4k files to evaluate a multi-tier cache. 

```
python3 convert_to_4k.py <IP_FOLDER> <OP_FOLDER>
```