# NGE Order Test

## Package Modify

>${PROJECT}\.virtual\lib\site-packages\swagger_spec_validator\validator20.py:53: SwaggerValidationWarning: Found "$ref: #/definitions/UserPreferences" with siblings that will be overwritten. See https://stackoverflow.com/a/48114924 for more information. (path #/definitions/User/properties/preferences)

origin: 
```python
keys_to_ignore = {'x-scope', '$ref', 'description'}
```

modified:
```python
keys_to_ignore = {'x-scope', '$ref', 'description', 'default'}
```