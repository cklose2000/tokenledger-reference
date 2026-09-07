select {{text('model')}} model,{{integer('generation')}} generation,{{day('release_date')}} release_date,ts released_at
from {{src('model_released')}}
