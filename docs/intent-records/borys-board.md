$id-4451695302335939
title: Borys board client
date: 2026/09/24
source: @ottojung
kind: requirement

Lubko includes a small `lubko-board` executable for interacting directly with the Borys shared issue/message board stored in Skrynia. Borys is shared durable coordination state only; it does not run or orchestrate Lubko work.

$id-5375330742269028
title: Minimal board model
date: 2026/09/24
source: @ottojung
kind: constraint

The Lubko board model is intentionally minimal: numbered issue threads with a title, open/closed state, timestamps, and chronological messages. Assignment/ownership, tags, priorities, labels, due dates, workflows, milestones, projects, and other project-management concepts are out of scope.

$id-6376081373327028
title: Clients pull and push board state themselves
date: 2026/09/24
source: @ottojung
kind: requirement

Clients and orchestrators interact with Borys by pulling board state and pushing their own changes through the shared Skrynia protocol. The board client is a thin API/CLI surface, not a runner, scheduler, daemon, or work-selection policy engine.
