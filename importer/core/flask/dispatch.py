from werkzeug.wsgi import DispatcherMiddleware
from werkzeug.exceptions import NotFound

import core.flask.clients
import core.flask.cloud_history
import core.flask.cloud
import core.flask.vend_history
import core.flask.integration

app = DispatcherMiddleware(
    NotFound(),
    {
       '/client': core.flask.clients.app,
       '/client/history': core.flask.cloud_history.app,
       '/cloud': core.flask.cloud.app,
       '/vend/history': core.flask.vend_history.app,
       '/integration': core.flask.integration.app
    }
)
