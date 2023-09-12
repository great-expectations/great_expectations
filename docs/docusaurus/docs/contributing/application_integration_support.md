---
title: Application integration support policy
---

Great Expectations (GX) provides the following support levels for integrated applications, operating systems, and programming languages:

<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" crossorigin="anonymous" referrerpolicy="no-referrer" />
<div>
    <ul style={{
        "list-style-type": "none"
    }}>
        <li><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> &nbsp; Fully tested and supported by GX</li>
        <li><i class="fas fa-circle" style={{color: "#ffc107"}}></i> &nbsp; Partially tested and supported by GX</li>
        <li><i class="fas fa-circle" style={{color: "#dc3545"}}></i> &nbsp; Untested and unsupported by GX </li>       
    </ul>
</div>

These support levels allow GX to better prioritize its response to application integration, operating system, and programming language issues. Formally defining the support policy also provides contributors with a better understanding of who is responsible for resolving application integration, operating system, and programming language issues.

## Support categories

The following are the types of support available for application integration, operating system, and programming language issues:

- **GX supported** - The GX Developer Relations (DevRel) team triages, reproduces, and then assigns issues to the appropriate GX Engineering team. Issues with application integration, operating system, and programming languages in maintenance mode are reviewed, but new features or functionality are not implemented as part of the resolution.

- **Community supported** - The GX DevRel team triages and reproduces issues, and then determines if GX should implement new features or functionality to resolve the issue. If the DevRel team determines that new features or functionality are not warranted, the GX community is responsible for issue resolution. If new features or functionality are required, DevRel assigns the issue to the appropriate GX Engineering team.

## GX versions

GX supports the current and two previous releases of GX. The following table defines who is responsible for providing GX version support.


| GX supported                             | Community supported                                                |
|------------------------------------------|--------------------------------------------------------------------|
| <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Current minor release. For example, 0.17.X    | <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Previous two minor releases. For example, 0.15.X, 0.16.X <i class="fas fa-circle" style={{color: "#dc3545"}}></i><br/>Three minor releases. For example 0.14.X              | 

## Operating systems

The following table defines the operating systems supported by GX.

| GX supported                             | Community supported                                                |
|------------------------------------------|--------------------------------------------------------------------|
| <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> macOS<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Linux    | <i class="fas fa-circle" style={{color: "#dc3545"}}></i> Windows              | 