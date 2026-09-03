[![](https://img.shields.io/nuget/v/soenneker.asyncs.semaphores.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.asyncs.semaphores/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.asyncs.semaphores/build-and-test.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.asyncs.semaphores/actions/workflows/build-and-test.yml)
[![](https://img.shields.io/nuget/dt/soenneker.asyncs.semaphores.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.asyncs.semaphores/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.asyncs.semaphores/codeql.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.asyncs.semaphores/actions/workflows/codeql.yml)

# ![](https://user-images.githubusercontent.com/4441470/224455560-91ed3ee7-f510-4041-a8d2-3fc093025112.png) Soenneker.Asyncs.Semaphores

A low-allocation asynchronous semaphore for bounded in-process concurrency.

The single-waiter path uses a direct handoff slot. Heavier contention spills into a zero-allocation intrusive MPSC queue backed by pooled `IValueTaskSource` waiters. Permit count and pending handoff ownership share one atomic state, allowing competing releasers to batch queue consumption with less synchronization.

## Installation

```
dotnet add package Soenneker.Asyncs.Semaphores
```

## Usage

Create a semaphore with the desired concurrency limit, then dispose every acquired lease:

```csharp
using Soenneker.Asyncs.Semaphores;

var semaphore = new AsyncSemaphore(4);

using SemaphoreLease lease = await semaphore.Acquire(cancellationToken);
await ProcessAsync(cancellationToken);
```

The single-argument constructor makes the initial and maximum permit counts equal. To begin with fewer permits available, specify both values:

```csharp
var semaphore = new AsyncSemaphore(initialCount: 0, maxCount: 4);
semaphore.Release();
```

`Release()` and `Release(int)` can signal permits explicitly. Disposing a lease releases its owned permit automatically, so do not manually release that same permit.

## Try without waiting

```csharp
if (semaphore.TryAcquire(out SemaphoreLease lease))
{
    using (lease)
    {
        // One permit is owned here.
    }
}
```

`Acquire` returns a `ValueTask<SemaphoreLease>` and the lease is a value type, so an immediately available permit requires no allocation. Contended waiter nodes are pooled for reuse. `SemaphoreLease.Dispose()` is idempotent for the same lease variable; because it is a mutable struct, do not dispose copies of a lease.
