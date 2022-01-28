# multiflight

`multiflight` is a tool like `singleflight`, but support multiple keys.

## Usage

```go
    var group = Group[int, string]{}

    result,err := group.Do(ctx, keys, func (ctx context.Context, keys []int)(map[int]string, error){
        ...
    })
    if err != nil {
        ...
    }
    for _,key := range keys {
        r, has := result[key]
        ...
    }
```
