$id-6724019853147602
title: Existing Lubko remains operational when general-purpose persistent storage is exhausted
date: 2026/09/21
source: @ottojung
kind: requirement

Exhausting the host's general-purpose persistent filesystem must not prevent an already-deployed Lubko supervisor and worker from starting, communicating with Supabase, executing jobs, or publishing their results.

This requirement applies to normal operation of an already-deployed Lubko instance. Running out of free bytes on the host's general-purpose persistent filesystem must not by itself make the deployed supervisor or worker unavailable or unable to carry jobs through the normal queue/result path.

$id-3157892460835174
title: Filesystem correctness must not depend on tmpfs
date: 2026/09/21
source: @ottojung
kind: constraint

Lubko must not assume that any filesystem path or mount available to it is tmpfs, memory-backed, or otherwise exempt from persistent-storage exhaustion.

In particular, correctness or availability under exhausted persistent storage must not depend on `/tmp`, `/run`, or any other conventional path being tmpfs. A deployment may provide tmpfs or other memory-backed storage, but Lubko must not require or infer that property merely from the filesystem path.
