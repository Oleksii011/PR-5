from app import createapp
from app.extensions import getdbconnection


app = createapp()


with app.app_context():

    pass

if __name == '__main':
    app.run(debug=True)
