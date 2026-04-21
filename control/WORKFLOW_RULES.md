RULE: NO CHAT-TO-NOTEPAD PYTHON FILE DELIVERY



Problem:

Manual transfer of full Python files from chat to Notepad is unreliable and causes file corruption, format drift, and wasted engineering cycles.



Mandatory protocol:

1\. Do not deliver full .py files through chat for manual Notepad replacement.

2\. Any local file change must be verified with:

&#x20;  type <path\_to\_file>

3\. Python execution is allowed only after file-content verification.

4\. Full file rewrites must go through GitHub/Codex workflow, not manual copy-paste.



Status:

LOCKED

