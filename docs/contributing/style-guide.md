The Python code and docstring format mostly follows Google's
[Python Style Guide](https://google.github.io/styleguide/pyguide.html){target=_blank},
but the pre-commit config is the authoritative source for code format
compliance.

**Nits:**

* Avoid imports in `__init__.py` (reduces the likelihood of circular imports).
* Prefer pure functions where possible.
* Define all class attributes inside `__init__` so all attributes are visible
  in one place. Attributes that are defined later can be set as `None`
  as a placeholder.
* Prefer f-strings (`#!python f'name: {name}`) over string format
  (`#!python 'name: {}'.format(name)`). Never use the `%` operator.
* Prefer [typing.NamedTuple][] over [collections.namedtuple][].
* Use lower-case and no punctuation for log messages, but use upper-case and
  punctuation for exception values.
  ```python
  logger.info(f'new connection opened to {address}')
  raise ValueError('Name must contain alphanumeric characters only.')
  ```
* Document all exceptions that may be raised by a function in the docstring.
