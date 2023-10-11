{{ objname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}

   {% block methods %}
   .. automethod:: __init__

   {% if methods %}
   .. rubric:: {{ _('Not Inherited Methods') }}

   .. autosummary::
      :toctree:
   {% for item in methods %}
      {% if item not in inherited_members %} 
      ~{{ name }}.{{ item }}
      {% endif %}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block attributes %}
   {% if attributes %}
   .. rubric:: {{ _('Attributes') }}

   .. autosummary::
   {% for item in attributes %}
      ~{{ name }}.{{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}


   {% block inherited %}
   .. rubric:: {{ _('Inherited methods') }}

   .. autosummary::
      :toctree:
   {% for item in methods %}
   {% if item in inherited_members %} 
      ~{{ name }}.{{ item }} 
   {% endif %}
   {% endfor %}
   {% endblock %}