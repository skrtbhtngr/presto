## PRESTO: PREwarming STOrage Caches for Improving I/O Performance in Virtualized Infrastructure

**Abstract**

Virtualized environments nowadays employ a hypervisor cache at each node to improve performance on the storage I/O path as well as alleviate some load on the underlying storage. This further benefits the environments having networked storage where the VM disk data is not necessarily available locally.

That being said, the cache itself is local to the node as it caters to the requests coming from the VMs on that particular node. From a performance point-of-view, the cached data in this hypervisor cache is as important as the backing data. A cold hypervisor cache would not result in drastic reduction in performance as compared to performance with no hypervisor cache present. But, there will be no improvement in the overall I/O performance, and the cache will fail to fulfil its purpose.

This study aims at identifying the scenarios which will render the hypervisor cache cold and coming up with methods which can aid in effectively "warming up" the otherwise cold cache. We consider the Nutanix HCI as the base model for our experiments.

---
### This project was a joint effort by Nutanix and SynerG@CSE, IITB.
---

The repo is structured as follows:

`code/` stores the project source files (without results and the dataset)

`mtpdocs/` contains various documents prepared as part of the project

`refs/` has a few materials (papers, pres., etc.) related to the project
