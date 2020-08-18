.. _contributing__levels_of_maturity:

##################
Levels of maturity
##################

Features and code within Great Expectations are separated into three levels of maturity: Experimental, Beta, and Production.

.. raw:: html

      <section class=""><div class="container my-container">
        <ul>
          <li><i class="fas fa-circle text-danger"></i> &nbsp; Experimental: Try, but do not rely</li>
          <li><i class="fas fa-circle text-warning"></i> &nbsp; Beta: Ready for early adopters</li>
          <li><i class="fas fa-check-circle text-success"></i> &nbsp; Production: Ready for general use</li>
        </ul>
      </div></section>


Being explicit about these levels allows us to enable experimentation, without creating unnecessary thrash when features and APIs evolve. It also helps streamline development, by giving contributors a clear, incremental path to create and improve the Great Expectations code base.

For users of Great Expectations, our goal is to enable informed decisions about when to adopt which features.

For contributors to Great Expectations, our goal is to channel creativity by always making the next step as clear as possible.

This grid provides guidelines for how the maintainers of Great Expectations evaluate levels of maturity. Maintainers will always exercise some discretion in determining when any given feature is ready to graduate to the next level. If you have ideas or suggestions for leveling up a specific feature, please raise them in Github issues, and we'll work with you to get there.

.. raw:: html
        
    <table class="legend-table">
    <tr>
        <td><br/><b>Criteria</b></td>
        <td style="width:25%"><b><i class="fas fa-circle text-danger"></i> &nbsp; Experimental</b><br/>Try, but do not rely</span></td>
        <td style="width:25%"><b><i class="fas fa-circle text-warning"></i> &nbsp; Beta</b><br/>Ready for early adopters</span></td>
        <td style="width:25%"><b><i class="fas fa-check-circle text-success"></i> &nbsp; Production</b><br/>Ready for general use</span></td>
    </tr>
    <tr>
        <td>API stability</td>
        <td style="padding-left:32px;">Unstable*</td>
        <td style="padding-left:32px;">Mostly Stable</td>
        <td style="padding-left:32px;">Stable</td>
    </tr>
    <tr>
        <td>Implementation completeness</td>
        <td style="padding-left:32px;">Minimal</td>
        <td style="padding-left:32px;">Partial**</td>
        <td style="padding-left:32px;">Complete</td>
    </tr>
    <tr>
        <td>Unit test coverage</td>
        <td style="padding-left:32px;">Minimal</td>
        <td style="padding-left:32px;">Partial</td>
        <td style="padding-left:32px;">Complete</td>
    </tr>
    <tr>
        <td>Integration/Infrastructure test coverage</td>
        <td style="padding-left:32px;">Minimal</td>
        <td style="padding-left:32px;">Partial</td>
        <td style="padding-left:32px;">Complete</td>
    </tr>
    <tr>
        <td>Documentation completeness</td>
        <td style="padding-left:32px;">Minimal</td>
        <td style="padding-left:32px;">Partial</td>
        <td style="padding-left:32px;">Complete</td>
    </tr>
    <tr>
        <td>Bug risk</td>
        <td style="padding-left:32px;">High</td>
        <td style="padding-left:32px;">Moderate</td>
        <td style="padding-left:32px;">Low</td>
    </tr>
    </table>
    <br/>

    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.11.2/css/all.min.css">            
    <style>
        .text-danger {
            color: #dc3545;
        }
        .text-warning {
            color: #ffc107;
        }
        .text-success {
            color: #28a745;
        }
        .legend-table td{
            border: 1px solid #ddd;
            padding: 5px;
        }
    </style>

\* Experimental classes log warning-level messages when initialized: “Warning: great_expectations.some_module.SomeClass is experimental. Methods, APIs, and core behavior may change in the future.”

** In the special case of Expectations, some gaps in implementation are allowed in beta (e.g. works in pandas and Spark, but not yet in SQLAlchemy; validation and rendering work, but not profiling yet)

