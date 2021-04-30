import wtiproj06_api_logic
import cherrypy
from wtiproj06_API import app
from paste.translogger import TransLogger

def run(api):
    log=TransLogger(api)
    cherrypy.tree.graft(log,'/')
    cherrypy.config.update({'engine.autoreload.on':True, 'log.screen':True, 'server.socket_port':9898, 'server.socket_host':'0.0.0.0'})
    cherrypy.engine.start()
    cherrypy.engine.block()

if __name__ == '__main__':
    api=wtiproj06_api_logic.api_logic()
    df, g = api.load(1)
    run(app)