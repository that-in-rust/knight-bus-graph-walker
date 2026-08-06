# SPC Submission

Created: 2026-07-23

Status: updated source-of-truth draft for the South Park Commons application. This document preserves the richer version supplied after the Airtable form-fill pass. It is not confirmation that the form has been submitted.

Related notes:

- [A008-spc-founder-application-draft.md](A008-spc-founder-application-draft.md)
- [A009-knight-walker-founder-pitch.md](A009-knight-walker-founder-pitch.md)

Primary project:

- [Knight Bus Graph Walker](https://github.com/that-in-rust/knight-bus-graph-walker)

## Project I Am Most Proud Of

Knight Bus Graph Walker is the project I am most proud of.

It tests a product/infra thesis: graph algorithms are useful, but graph OLAP runtimes often load too much into RAM, which makes people avoid using graph algorithms altogether. For example, for a 50 GB dataset, people may need to rent a very large server that costs a lot of money. My main aim is to take this to a point where a 50 GB dataset can run on a 16 GB RAM CPU instead of a 128 GB RAM server, without fear of the job failing.

There is enough Reddit-style reference and practitioner pain around this problem: graph algorithms are powerful, but the memory footprint and cloud-cost anxiety make them feel too risky for routine OLAP use.

Repo: [https://github.com/that-in-rust/knight-bus-graph-walker](https://github.com/that-in-rust/knight-bus-graph-walker)

I am building it via Devin after joining the Devin Ambassador program.

This rethinks graph execution from the storage layer up: reshape graph OLAP storage to mirror graph algorithms' access patterns. The POC uses precompiled forward/reverse CSR-style adjacency patterns; on the tracked 2GB dataset it used 4.5x lower runtime RAM than Neo4j while returning the same answers.

## Artifacts That Show High-Quality Work

1. Knight Bus Graph Walker repo: [https://github.com/that-in-rust/knight-bus-graph-walker](https://github.com/that-in-rust/knight-bus-graph-walker)
2. Public thesis threads:
   - [https://x.com/amuldotexe/status/2068194152941326836](https://x.com/amuldotexe/status/2068194152941326836)
   - [https://x.com/amuldotexe/status/2073247774674710970](https://x.com/amuldotexe/status/2073247774674710970)
   - These explain why RAM-heavy graph OLAP is expensive and why I am rethinking graph execution from the storage layer up.
3. Apache Iggy PR: [https://github.com/apache/iggy/pull/2815](https://github.com/apache/iggy/pull/2815)
   - Rust OSS contribution to a fast-evolving Kafka replacement after grokking control/data flow.

## Funding Status

No. I have not raised funding or actively fundraised for Knight Bus yet.

I have been building the technical proof and sharpening the thesis first: whether graph execution can be redesigned from the storage layer up so graph algorithms become much cheaper to run in cloud environments.

## Problem Spaces

### 1. Storage-Specialized Graph Analytics Runtime

Graph algorithms are useful for fraud, recommendations, dependency analysis, risk, supply-chain analysis, and AI/code graphs, but OLAP graph runtimes are often too RAM-heavy for routine cloud use.

The cost is extremely prohibitive and unreliable in terms of execution. It can cost hundreds of dollars per hour, which is a key revenue source for a company like Neo4j. Neo4j makes hundreds of millions in annual revenue, and part of that business rests on the fact that graph workloads are operationally expensive and specialized.

It is time to make graph algorithms as common as aggregation algorithms that people run on relational databases or tabular data.

Knight Walker tests whether CSR-style storage and direct traversal arithmetic can make graph walking cheap enough to become a practical OLAP option.

### 2. AI-Native Codebase Intelligence

Parseltongue was built out of my own frustration with LLMs not being able to navigate large codebases.

It takes a codebase and converts it into a tree-sitter-driven public interface signature graph, which helps build a higher-level map of the codebase itself.

Obviously, LLMs have grown better at exploring large codebases. However, given the amount of code we are writing every day, I think there is a step jump needed in how we look at reviewing code: not from a syntax point of view, but from an interface graph point of view.

The abstraction at which you write code for usual CRUD apps has changed. While it might not change for infra, where each line of code is important for deciding if this is indeed the most optimal way to do it, for CRUD applications and large backend services the pain point is at the reviewer end.

This cannot be solved by the reviewer reading 5,000 lines of code. There has to be an aggregated version of the codebase and the git diff that the reviewer can look at.

## Why I Am The Right Person

I have an unusual fit for deep technical work using LLMs.

I started my work as an analyst at Mu Sigma, and over the next five years I was denormalizing large swathes of data and converting it into standardized forms for insights and regression models. This gave me an uncommon insight into how data is stored, aggregated, and eventually consumed by models as well as humans.

That eventually led me to game design and game product management at EA Sports. Over the next five years, I learned a lot more about human behavior and how to look at it at an aggregated level via telemetry.

Then I worked for six years as a Senior Product Manager at Target, navigating a large organization and developing insight into how enterprises buy and use software inside large revenue machines.

At the end of it all, I realized that with LLMs, somebody curious like me can do deeply technical work and marry it with end-customer insight, both at qualitative and quantitative levels.

Most people playing the deep tech/infra game are only good at one of these trades. Engineering was the next trade to master. That is why I chose to take a sabbatical to do open source contributions in Rust, got a PR merged in Apache Iggy, and then got a contract as a product engineer at a Series A startup.

While working here on CRUD applications, I pitched Devin for tokens to support the Knight Bus Graph Walker initiative, and they made me a Devin Ambassador to enable the same.

## What I Want To Build Long-Term

I want to write software that can last a decade, because that would be really cool.

Not many people want to do that. They want to write quick software that makes quick money. If LLMs are so good at writing code, why should I be writing simple applications? I should write something that has not been rewritten for 15 years.

The graph runtime is just phase 1.

I believe that in the coming years the infra of the internet will be rewritten by those deliberate enough to go into the details of it.

My ambition is to write a browser sans the mess of HTML, CSS, and JS, which has monopolized the way we interact with the internet. I also want to write an OS customized for the applications it runs.

For example: what if you had an OS optimized for running graph runtimes? Or an OS optimized for running Spark-like aggregation loads?

A lot of such infra-level software could reduce the cost of cloud 10x, but more importantly, it could offer higher value to organizations because of lower latency and higher efficiency. I want to be one of the people building that.

## Why I Am Different From A Pure Deep-Tech Founder

I want to emphasize the difference between me and an extremely deep technical person.

The deep technical person is often very intent on making something more efficient, having low latency, and so on. That matters. But they may not have the level of empathy I have for customers, which can lead to more adoption.

There are many graph runtimes being written right now which will not be able to replace Neo4j because they are built by people who are not talking to customers.

I am not one of them.

I have already chosen this problem treatment for graph runtimes knowing that there is a pain point there. Writing something cool means the planet should adopt it, and that is what I aim for in all the infra software that I am writing.

It will be successful, or called successful, only if the planet adopts it for solving a real pain point.

## Progress Made

Built an MIT-licensed Rust POC for graph walking algorithms:

- [https://github.com/that-in-rust/knight-bus-graph-walker](https://github.com/that-in-rust/knight-bus-graph-walker)

On a 2GB dataset it used 4.5x lower runtime RAM than Neo4j.

I pitched Devin for tokens to support the Knight Bus Graph Walker initiative, and they made me a Devin Ambassador to enable the same. They are sponsoring $700 credits plus a $200 max plan per month for this, along with useful product feedback.

## Relevant Links

- Knight Walker repo: [https://github.com/that-in-rust/knight-bus-graph-walker](https://github.com/that-in-rust/knight-bus-graph-walker)
- Storage-layer-up / Devin Ambassador thread: [https://x.com/amuldotexe/status/2073247774674710970](https://x.com/amuldotexe/status/2073247774674710970)
- Original RAM-heavy graph OLAP thesis: [https://x.com/amuldotexe/status/2068194152941326836](https://x.com/amuldotexe/status/2068194152941326836)

## What I Need Next

I actually just need more time to work on this idea and build a larger POC surface area.

I have already identified the top seven algorithms that account for 80 to 90% of graph OLAP use cases of Neo4j. Once I am able to build that out, it will be easy to prove that it works because I am directly comparing it against Neo4j.

If I had some extra money, I would probably hire a few interns to help me develop this faster.

Once I reach enough clarity on the GTM and monetization side, the only people I would want to hire would probably be on the B2B SaaS company SOP side: legal, HR, compliance, and engineering operations.

## Company Shape

I am not hoping for this initiative to become a large company.

I think we can go really far with just a five-person company. I want to be involved in all the selling, and I want to be involved in building.

While that might not have been true a few years ago, with LLMs I think I can do both. Maybe I am wrong about this, but I would rather test myself on this with some stretch before hiring someone.

## Why SPC

I have already met some of the folks at SPC and attended events, which indicate to me that SPC has a high talent density of people who can point me in the right direction.

The main help I will seek from SPC is not as much on the technical side as on figuring out a GTM strategy for adoption of an OSS product.

Building the graph runtime is hard, but I do not think that is the hardest part for me because the core thesis is proven. The harder part is getting the planet to adopt it.

This is where I want as much help as possible because building a cool graph runtime is of no use if enough people do not use it.

Even MIT-licensed software has a high friction bar for adoption, especially at the enterprise level.

I would also want guidance on how to monetize it. I have not worked on monetization of open source tools. I have read some precedent, but it would be great to have guidance or pointers on how to figure it out while I am doing GTM.

In order to sustain such infra initiatives, it is important to figure out the monetization front early.

## Open Tab Questions And Current Answers

Captured from the open Airtable tab on 2026-07-23. This section preserves the live form questions and current filled answers so future edits can map directly back to the SPC application.

### South Park Commons Application

```text
Building a company is challenging, especially in the initial stages. SPC supports founders at their earliest "-1" stage of exploration and at every phase after that through our community, programming, and funding in the form of $1M - $10M investments.

Fill out the application below and we’ll let you know within the next few weeks if we’d like to move forward with the interview process; we aim to make application decisions within a month.  Read more about SPC in our FAQ.
```

### Getting started

**Question copied as-is:**

```text
Which best describes where you are in your journey today?
*

 To connect you with the right programs and resources, help us understand where you are today. Note that many founders start with us in the -1 exploration phase and then move directly into founding and funding their companies with us.
```

**Current answer:**

```text
Founder Fellowship: I'm starting or started a company and am ready to pitch and fundraise
```

### About You

**Question copied as-is:**

```text
Full Name
*
```

**Current answer:**

```text
Amul Badjatya
```

**Question copied as-is:**

```text
Email
*
```

**Current answer:**

```text
amul.exe@gmail.com
```

**Question copied as-is:**

```text
Phone number
*
```

**Current answer:**

```text
+91 96112 56611
```

**Question copied as-is:**

```text
LinkedIn profile
*
```

**Current answer:**

```text
https://www.linkedin.com/in/amul2024/
```

**Question copied as-is:**

```text
Where will you be based?
*
```

**Current answer:**

```text
Bengaluru
```

**Question copied as-is:**

```text
How did you hear about the application?
*
From an SPC Community Member
From an SPC Staff Member
Friend (not a current or past SPC member)
X / Social Media
SPC Blog
SPC Event
Other
```

**Current answer:**

```text
X / Social Media
```

**Question copied as-is:**

```text
Please briefly elaborate on the above

We'd love to know who referred you, or which account, post, event, or other resource led you here.
```

**Current answer:**

```text
Prateek Mehta's X post about the SPC Founder Fellowship and building what only you can build: https://x.com/prateekmehta42/status/2079148824082432163
```

### Background and Prior Work

**Question copied as-is:**

```text
What personal or professional product, project, or achievement are you most proud of? Please link us to it and briefly tell us about it.
*

Possible examples: gold medal competitive programmer, Division 1 college athlete, led the GPT-3 dev team, built a $500k revenue window washing business in college, etc. Feel free to brag. Keep under 1000 characters.
```

**Current answer copied from the open tab:**

```text
Knight Bus Graph Walker is the project I am most proud of : It tests a product/infra thesis that though graph algorithms are useful, but graph OLAP runtimes often load too much into RAM, and makes people avoid using the graph algorithms altogether because for a 50 GB dataset you need a very huge server on rent that costs a lot of money. My main aim is to take this to a point where a 50 GB dataset can be run on a 16 GB RAM CPU instead of a 128 GB RAM server without fear of the job failing. Enough Reddit reference for this pain point . Repo: https://github.com/that-in-rust/knight-bus-graph-walker. I am building it via Devin after joining the Devin Ambassador program. This rethinks graph execution from the storage layer up: reshape graph OLAP storage to mirror graph algorithms' access patterns. The POC uses precompiled forward/reverse CSR-style adjacency patterns; on the tracked 2GB dataset it used 4.5x lower runtime RAM than Neo4j while returning the same answers.
```

**Question copied as-is:**

```text
Share 2-3 artifacts that illustrate your ability to do high-quality work.
*

Link and explain any side projects, curiosities, or experiments you've developed that might help us understand how you think or what you’re drawn to. Keep under 1000 characters.
```

**Current answer copied from the open tab:**

```text
1. Knight Bus Graph Walker repo: https://github.com/that-in-rust/knight-bus-graph-walker 2. Public thesis threads: https://x.com/amuldotexe/status/2068194152941326836 and https://x.com/amuldotexe/status/2073247774674710970 - why RAM-heavy graph OLAP is expensive and why I am rethinking graph execution from the storage layer up. 3. Apache Iggy PR: https://github.com/apache/iggy/pull/2815 - Rust OSS contribution to a fast-evolving Kafka replacement after grokking control/data flow.
```

### Founder Fellowship & Fundraising

**Form copy copied as-is:**

```text
Based on your answers to the Getting Started question above, we think the Founder Fellowship & Fundraising application is the best path for you.

Note that our next Founder Fellowship program will launch in late summer / early fall of 2026; we are reviewing applications now and may providing funding to companies at any time.
```

**Question copied as-is:**

```text
Do you have a founding team?
*

Only one Founder should fill out the application.
```

**Current answer:**

```text
No, I'm a solo founder
```

**Question copied as-is:**

```text
Have you raised any funding or actively fundraised in the last 6 months for this company?
*

If yes, please explain. If no, feel free to skip.
```

**Current answer copied from the open tab:**

```text
No. I have not raised funding or actively fundraised for Knight Bus yet. I have been building the technical proof and sharpening the thesis first: whether graph execution can be redesigned from the storage layer up so graph algorithms become much cheaper to run in cloud environments.
```

**Question copied as-is:**

```text
What primary problem space(s) are you pursuing and why is it important?
*

We're okay with you listing several ideas, including ones that are a bit "out there." We look for founders who are highly creative, and many of our successful companies ended up working on ideas that weren't ones the team started with. This is your elevator pitch.
```

**Current answer copied from the open tab:**

```text
1. Storage-specialized graph analytics runtime: graph algorithms are useful for fraud, recommendations, dependency analysis, risk, supply-chain and AI/code graphs, but OLAP graph runtimes are often too RAM-heavy for routine cloud use. Cost is extremely prohibitive and unreliable in terms of execution. It costs hundreds of dollars per hour, which is what is the key revenue source for somebody like Neo4j who makes $200 million a year . It is time we should make the graph algorithms became as common as the aggregation algorithms that you run on relational databases or tabular data. Knight Walker tests whether CSR-style storage and direct traversal arithmetic can make graph walking cheap enough to be via OLAP options. 2. AI-native codebase intelligence: Parseltongue I was built out of my own frustration with LLMs not being able to navigate large code bases. Basically took all of your code base and converted it into a tree sitter driven public interface signature graph, which helps you build a higher level map of the code base itself . Obviously LLMs have grown better at exploring large code bases. However given the amount of code we are writing every day, I think there's a step jump needed in the way we look at reviewing code, not from a syntax point of view but from an interface graph point of view . The abstraction at which you write code for usual CRUD apps has changed. While it might not change for infra, where each line of code is important in deciding if this is indeed the most optimal way to do it. For CRUD applications for large backend services, the pain point is at the end of the reviewer and it cannot be solved by the reviewer reviewing the 5,000 lines of code. There has to be an aggregated version of the codebase and the git diff that the reviewer can look at
```

**Question copied as-is:**

```text
What expertise do you have related to this idea? Why are you the right person to work on this?
*
```

**Current answer copied from the open tab:**

```text
I have an unusual fit for deep technical work using LLMs: I started my work as an analyst at Mu Sigma and over the next 5 years I was de-normalizing large swathes of data and converting it into standardized form for insights and regression models. This gave me an uncommon insight into how data is stored and aggregated and is eventually consumed by models as well as humans. It eventually led me to game design and game product management at EA Sports and the next five years I learned a lot more about human behavior and how to look at it at an aggregated level via telemetry. Then I worked for 6 years as a Senior Product Manager at Target - navigating a large organization and got some insights into how enterprises sold software to such large revenue machines . At the end of it all I realized that with LLMs somebody curious like me can do deeply technical work and marry it with end customer insight, both at qualitative and quantitative levels . Most people playing the deep tech/ infra game are only good at one of these trades. Engineering was the next trade to master - It is why I chose to take a sabbatical to do open source contributions in Rust, got a PR merged in Apache Iggy, and then got a contract as a product engineer at a series A start up. While working here on CRUD applications, I pitched Devin for tokens to support Knight Bus Graph Walker initiative and they made me a Devin Ambassador to enable the same. I want to write software which can last a decade - because that would be really cool. Not many people want to do that. They want to write quick software that makes quick money. If LLMs are so good at writing code, why should I be writing simple applications? I should write something that has not been re-written for 15 years. The graph run time is just phase 1. I believe that in the coming years the Info of the Internet will be rewritten by those deliberate enough to go into the details of it. My ambition is to write a browser sans the mess of HTML CSS JS which has monopolized the way we interact with the internet. And I want to write an OS customized for the applications it runs. For e.g. What if you had an OS which was optimized for running graph run times? Or an OS optimized for running Spark like aggregation loads. A lot of such infra-level software will reduce the cost of cloud 10x but more importantly it will offer higher value to organizations because of lower latency and higher efficiency. And I want to be one of such people. I want to emphasize the difference between me and an extremely deep tech person is that the deep tech person is very intent on making it more efficient and having low latency and so on. They do not have the level of empathy I have for customers, which can lead to more adoption . There are many graph runtimes being written right now which won't be able to replace Neo4j because they all have people who are not talking to their customers. I'm not one of them. I've already chosen this problem treatment for graph runtimes, knowing that there is a pain point there . Writing something cool means that the planet should adopt it and that is what I aim for in all the Info Software that I'm writing. It will be successful or called successful if the planet adopts it and my main aim is to make sure that the planet adopts it for solving a real pain point
```

**Question copied as-is:**

```text
What progress have you made on this idea?
*
```

**Current answer copied from the open tab:**

```text
Built an MIT-licensed Rust POC for graph walking algorithms: https://github.com/that-in-rust/knight-bus-graph-walker. On a 2GB dataset it used 4.5x lower runtime RAM than Neo4j. I pitched Devin for tokens to support Knight Bus Graph Walker initiative and they made me a Devin Ambassador to enable the same. They are sponsoring $700 credits + a $200 max plan per month for this (and some useful product feedback)
```

**Question copied as-is:**

```text
Share links to any artifacts you've built or published related to this idea.
*

This can be a demo, live prototype, memo, or any artifact.
```

**Current answer copied from the open tab:**

```text
Knight Walker repo: https://github.com/that-in-rust/knight-bus-graph-walker Storage-layer-up / Devin Ambassador thread: https://x.com/amuldotexe/status/2073247774674710970 Original RAM-heavy graph OLAP thesis: https://x.com/amuldotexe/status/2068194152941326836
```

**Question copied as-is:**

```text
Who are the next 2-3 people you'd want to and could recruit to your team and why?
*

Help us understand how you think about talent density.
```

**Current answer copied from the open tab:**

```text
I actually just need more time to work on this idea and build a larger POC surface area. I have already identified the top seven algorithms which account for 80 to 90% of graph OLAP use cases of Neo4j . Once I'm able to build that out and it is easy to prove that it works because I'm directly comparing it against Neo4j . If I had some extra money I would probably hire a few interns to assist me in developing this faster. Once I reach enough clarity on the GTM and monetization side, then probably the only people I would like to hire would be on running the B2B SAAS company SOP side : Legal, HR, Compliance Engineering; I'm not hoping for this initiative to become a large company. I think we can go really far with just a five-person company . I want to be involved in all the selling and I want to be involved in building. While that might not have been true a few years ago, with LLMs I think I can do both. Maybe I am wrong about this but I would rather test myself on this with some stretch before hiring someone .
```

### Staying in touch

**Question copied as-is:**

```text
Regardless of the outcome of your application, would you like to stay in touch with SPC to hear about future events, updates, or interesting roles at SPC companies?
```

**Current answer:**

```text
Checked
```

**Question copied as-is:**

```text
Anything else to add?
```

**Current answer copied from the open tab:**

```text
I have already met some of the folks at SPC and attended events, which indicate to me that you have a high talent density of people who can point me in the right direction . The main help I will seek from SPC is not as much on the technical side as much as how to figure out a GTM strategy for adoption of an OSS product? Well building the graph runtime is hard. I don't think that is very hard for me because the core thesis is proven. The harder part is getting the planet to adopt it . And this is where I want to get as much help as possible because building a cool graph runtime is of no use if you can't get enough people to use it . Even MIT License software has a high bar of friction for adoption, especially at enterprise level . That and maybe some guidance on how to monetize it. I haven't worked on monetization of open source tools. I have read some precedence but it will be great to have some guidance or pointers on how to figure it out while I am doing the GTM . Because in order to sustain such infra initiatives it is important to figure out the monetization front early on .
```

## Raw Copy-Paste Links

```text
Knight Bus Graph Walker repo: https://github.com/that-in-rust/knight-bus-graph-walker
Public thesis threads:
https://x.com/amuldotexe/status/2068194152941326836
https://x.com/amuldotexe/status/2073247774674710970
Apache Iggy PR: https://github.com/apache/iggy/pull/2815
```
