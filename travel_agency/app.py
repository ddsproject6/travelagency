from flask import Flask,flash,render_template,request,g, session, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event
from forms import  LoginForm, RegistrationForm
from flask_mail import Mail
import json, os, math
from datetime import datetime,timedelta,date
from flask_login import UserMixin, LoginManager, login_required, login_user, current_user, logout_user
from werkzeug.security import generate_password_hash, check_password_hash
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, IntegerField, TextAreaField
from wtforms.validators import DataRequired, Email, EqualTo
from sqlalchemy.engine import Engine
from client import Client
from threading import Thread
import time






app=Flask(__name__)
# Initialize the client
client = Client("mserver_config.json")

# Define a function to run the client's connection logic
def run_client():
    # while True:
    #     try:
    #         client.connect_to_master()
    #         time.sleep(60)
    #     except Exception as e:
    #         print(f"Error in client connection: {e}")

    client.connect_to_master()

# Start the client thread
client_thread = Thread(target=run_client, daemon=True)
client_thread.start()








db_path = os.path.join(os.getcwd(), 'server1_files', 'test.db')
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{db_path}"
app.config['SQLALCHEMY_ECHO']=True
app.config["SECRET_KEY"] = "thisismysecretkey#"

app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=False
db = SQLAlchemy(app)


login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view='login'

#admin = Admin(app)



@event.listens_for(Engine,"connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
  cursor=dbapi_connection.cursor()
  cursor.execute("PRAGMA foreign_keys=ON")
  cursor.close()






class User(UserMixin,db.Model):
    __tablename__="user"
    id = db.Column(db.Integer, primary_key=True)
    email=db.Column(db.String(125),unique=True,nullable=False)
    firstname=db.Column(db.String(64),index=True,unique=True,nullable=False)
    lastname=db.Column(db.String(64))
    number=db.Column(db.Integer, unique=True)
    password_hash = db.Column(db.String(128))
    package=db.relationship('Package',backref=db.backref('pkgs'),lazy='dynamic')
    payment=db.relationship('Payment',backref=db.backref('pay'),lazy='dynamic')
    hotel=db.relationship('Hotel',backref=db.backref('hotl'),lazy='dynamic')
    transp=db.relationship('Transport',backref=db.backref('trans'),lazy='dynamic')
    
   

    def __repr__(self): 
            return '<User {}>'.format(self.username)

    def set_password(self, password):
            self.password_hash = generate_password_hash(password) 

    def check_password(self, password):
            return check_password_hash(self.password_hash, password)   

    


class Contact(db.Model):
    __tablename__="contact"
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(80),nullable=False)
    name=db.Column(db.String(12), nullable=False)
    phone_num = db.Column(db.String(12), nullable=False)
    msg = db.Column(db.String(320), nullable=False)
    date = db.Column(db.Integer, nullable=True)
    

class Feedback(db.Model):
    __tablename__="feedback"
    id = db.Column(db.Integer, primary_key=True)
    
    username=db.Column(db.String(64),index=True,unique=True,nullable=False)
    email = db.Column(db.String(80),nullable=False)
    scale=db.Column(db.String(64))
    rating=db.Column(db.String(64))
    feedback=db.Column(db.String(320))
    

class Package(db.Model):
    __tablename__="package"
    id = db.Column(db.Integer, primary_key=True)
    email=db.Column(db.String(80),nullable=False)
    package_name=db.Column(db.String(80), nullable=False)
    place=db.Column(db.String(80),nullable=False)
    numOfDays=db.Column(db.String(80), nullable=False)
    estimated_cost=db.Column(db.String(80), nullable=False)
    date_booked=db.Column(db.String(80),default = datetime.now,nullable=False)
    userid=db.Column(db.Integer, db.ForeignKey('user.id',onupdate="cascade"))
    




class Hotel(db.Model):
    __tablename__="hotel"
    id = db.Column(db.Integer, primary_key=True)
    email=db.Column(db.String(80),nullable=False)
    checkin_date=db.Column(db.String(80),default = datetime.date, nullable=False)
    checkout_date=db.Column(db.String(80),default = datetime.date, nullable=False)
    place=db.Column(db.String(80), nullable=False)
    cost=db.Column(db.String(80), nullable=False)
    star_type=db.Column(db.String(80), nullable=False)
    userid=db.Column(db.Integer,db.ForeignKey('user.id',onupdate="cascade"))


class Transport(db.Model):
    __tablename__="transport"
    id = db.Column(db.Integer, primary_key=True)
    email=db.Column(db.String(80),nullable=False)
    mode_of_transport=db.Column(db.String(80), nullable=False)
    trvcost=db.Column(db.String(80),nullable=False)
    start_date=db.Column(db.String(80),default = datetime.date, nullable=False)
    boarding_place=db.Column(db.String(80), nullable=False)
    place=db.Column(db.String(80), nullable=False)
    boarding_time=db.Column(db.String(80), nullable=False,default = datetime.time)
    userid=db.Column(db.Integer, db.ForeignKey('user.id',onupdate="cascade"))
    


class Payment(db.Model):
    id=db.Column(db.Integer,primary_key=True)
    email=db.Column(db.String(80),nullable=False)
    total_amount=db.Column(db.String(80),nullable=False)
    bookedpack=db.Column(db.String(80),nullable=False)
    userid = db.Column(db.Integer, db.ForeignKey('user.id',onupdate="cascade"))
    
    

# db.create_all()


@app.route('/')
def home():
    return render_template('home.html')

@app.route('/dashboard')
def dashboard():
    usr=User.query.all()
    packg=Package.query.all()
    hotl=Hotel.query.all()
    trans=Transport.query.all()
    cont=Contact.query.all()
    feedb=Feedback.query.all()
    paym=Payment.query.all()
    return render_template('dashboard.html',usr=usr,packg=packg,hotl=hotl,cont=cont,feedb=feedb,paym=paym,trans=trans)


@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id)  

@app.route('/signup',methods=['GET','POST'])
def signup():
    form = RegistrationForm(csrf_enabled=False)
    
    if form.validate_on_submit():
        user = User(firstname=form.firstname.data, email=form.email.data, lastname=form.lastname.data, number=form.number.data)
        user.set_password(form.password.data)
        db.session.add(user)
        db.session.commit()
        return redirect(url_for('login'))
    return render_template('signup.html', title='Signup', form=form)   


@login_manager.user_loader
def load_user(user_id):
  return User.query.get(int(user_id))

# login route
@app.route('/login', methods=['GET','POST'])
def login():
    form = LoginForm(csrf_enabled=False)
    if form.validate_on_submit():
        if form.email.data =='admin@gmail.com' and form.password.data=='admin1234':
            session['user']='Admin'
            return redirect(url_for('dashboard'))
        else:
    # Query for user email 
            email = form.email.data
            chunk_address = client.request_table("user")
            print(chunk_address)
            query_result = client.retrieve_table_from_chunk_server("user", chunk_address, email)

            # user = User.query.filter_by(email=form.email.data).first()
            print(query_result)
            user_data = json.loads(query_result)

        # check if a user was found and the form password matches here:
            user = User(
                id=user_data['id'],
                email=user_data['email'],
                firstname=user_data['firstname'],
                lastname=user_data['lastname'],
                number=user_data['number'],
                password_hash=user_data['password_hash']
            )
            if user and user.check_password(form.password.data):
        
        # login user here:
                
                login_user(user, remember=form.remember.data)
                render_template('index.html',current_user=user)
                next_page = url_for('index')
                flash('Login Successful ')
                return redirect(next_page) if next_page else redirect(url_for('index', _external=True, _scheme='https'))
                
            else:
                flash('Invalid Credentials!!')
                return redirect(url_for('login',_external=True))

    return render_template('login.html', form=form)    
   


@app.route('/about')
@login_required
def about():
    return render_template('about.html')



@app.route('/contact',methods=['GET','POST'])
def contact():
    if(request.method=='POST'):
            '''Add entry to the database'''
            email = request.form.get('email')
            name = request.form.get('name')
            phone = request.form.get('phone')
            message = request.form.get('message')
            try:
                contactme = Contact(email = email,name=name, phone_num = phone, msg = message, date= datetime.now() )
                db.session.add(contactme)
                db.session.commit()
                flash('We will get in touch soon!')
            except:
                flash('Sorry Could not contact us...Please try again!! ')
    return render_template('contact.html')
        

@app.route('/index')
@login_required
def index():
    return render_template('index.html')



@app.route('/package',methods=['GET','POST'])
@login_required
def package():
    packages = {
        'Delhi': {'package_name': 'Delhi Package', 'place': 'Delhi', 'numOfDays': 10, 'estimated_cost': 20000},
        'Mumbai': {'package_name': 'Mumbai Package', 'place': 'Mumbai', 'numOfDays': 10, 'estimated_cost': 23000},
        'Bangalore': {'package_name': 'Bangalore Package', 'place': 'Bangalore', 'numOfDays': 10, 'estimated_cost': 17500},
        'Agra': {'package_name': 'Agra Package', 'place': 'Agra', 'numOfDays': 8, 'estimated_cost': 13000},
        'Amritsar': {'package_name': 'Amritsar Package', 'place': 'Amritsar', 'numOfDays': 8, 'estimated_cost': 8000},
        'Chennai': {'package_name': 'Chennai Package', 'place': 'Chennai', 'numOfDays': 8, 'estimated_cost': 10000},
        'Hedrabad': {'package_name': 'Hedrabad Package', 'place': 'Hedrabad', 'numOfDays': 8, 'estimated_cost': 9000},
        'Gujrat': {'package_name': 'Gujrat Package', 'place': 'Gujrat', 'numOfDays': 8, 'estimated_cost': 7000},
        'Mysore': {'package_name': 'Mysore Package', 'place': 'Mysore', 'numOfDays': 5, 'estimated_cost': 5100},
        'Dehradun': {'package_name': 'Dehradun Package', 'place': 'Dehradun', 'numOfDays': 5, 'estimated_cost': 5000},
        'Goa': {'package_name': 'Goa Package', 'place': 'Goa', 'numOfDays': 5, 'estimated_cost': 5400},
        'Jaipur': {'package_name': 'Jaipur Package', 'place': 'Jaipur', 'numOfDays': 10, 'estimated_cost': 10500},
        'Kashmir': {'package_name': 'Kashmir Package', 'place': 'Kashmir', 'numOfDays': 8, 'estimated_cost': 11000},
        'Kerela': {'package_name': 'Kerela Package', 'place': 'Kerela', 'numOfDays': 8, 'estimated_cost': 7300},
        'Bhopal': {'package_name': 'Bhopal Package', 'place': 'Bhopal', 'numOfDays': 5, 'estimated_cost': 5750},
        'Kullu Manali': {'package_name': 'Manali Package', 'place': 'Kullu Manali', 'numOfDays': 10, 'estimated_cost': 8200},
        'Ooty': {'package_name': 'Ooty Package', 'place': 'Ooty', 'numOfDays': 4, 'estimated_cost': 4800},
        'Orrissa': {'package_name': 'Orrissa Package', 'place': 'Orrissa', 'numOfDays': 6, 'estimated_cost': 5900},
        'Sikkim': {'package_name': 'Sikkim Package', 'place': 'Sikkim', 'numOfDays': 5, 'estimated_cost': 6800},
        'Shimla': {'package_name': 'Shimla Package', 'place': 'Shimla', 'numOfDays': 8, 'estimated_cost': 9000},
    }
    
    if request.method == 'POST':
        selected_package = request.form['submit_button']
        
        # Fetch package details from dictionary
        package_details = packages.get(selected_package, packages['Shimla'])  # Default to Shimla if not found
            
        
        try:
            # Save the package details into the database
            new_package = Package(
                email=current_user.email,
                package_name=package_details['package_name'],
                place=package_details['place'],
                numOfDays=package_details['numOfDays'],
                estimated_cost=package_details['estimated_cost'],
                date_booked=datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                pkgs=current_user
            )
            db.session.add(new_package)
            db.session.commit()

            flash('Travel Package is added')
            return redirect(url_for('hotel',_external=True))
        except:
            flash('There was problem adding package')

    
    return render_template('package.html')


@app.route('/hotel',methods=['GET','POST'])
@login_required
def hotel():
    if(request.method=='POST'):
            '''Add entry to the database'''
            pk_days=db.session.query(Package).filter(current_user.email==Package.email).all()
            checkin_date = request.form.get('startdate')
            pn,pl=[],[]

            
            #iterate through package
            for i in  pk_days:
                pn.append(i.numOfDays)
                pl.append(i.place)
            

            i=0
            while i< len(pn):
                if i==len(pn)-1: 
                    pdays=pn[i]
                    place=pl[i]
                    break
                else:
                    i+=1

            
            today = date.today()
            try:
                startdate=datetime.strptime(checkin_date, '%Y-%m-%d').date()
                if startdate >= today:
                    checkin_date=startdate
                    checkout_date= startdate + timedelta(int(pdays))
                    cost= request.form.get('cost')
                    star_type=request.form.get('example')
                else:
                    raise Exception
                
                accomandation = Hotel(email = current_user.email,checkin_date=checkin_date, checkout_date =checkout_date,place=place, cost= cost,star_type=star_type,hotl=current_user )
                db.session.add(accomandation)
                db.session.commit()
                flash('Travel Accomodation data added ')
                return redirect(url_for('transport',_external=True))
            except:
                flash('Accomodation could not be added check details Entered')
    
    return render_template('hotel.html') 

@app.route('/transport',methods=['GET','POST'])
@login_required
def transport():
    if(request.method=='POST'):
            pn=[]
            pkg=db.session.query(Package).filter(Package.email==current_user.email).all()
            transportmode = request.form.get('Mode of Travel')
            if transportmode=='Flight':
                cost=7000
            elif transportmode=='Bus':
                cost=1000
            elif transportmode=='Train':
                cost=700
            else:
                cost=1000
            startdate= request.form.get('s_date')
            boarding_place= request.form.get('myCountry')
            boarding_time=request.form.get('r_time')

            today = date.today()
            #iterate through package
            for i in  pkg:
                pn.append(i.place)
            

            i=0
            while i< len(pn):
                if i==len(pn)-1: 
                    p=pn[i]
                    break
                else:
                    i+=1


            
            try: 
                startdate=datetime.strptime(startdate, '%Y-%m-%d').date()
                if startdate >= today:
                    start_date=startdate
                else:
                    raise Exception

                transport=Transport(email=current_user.email,mode_of_transport=transportmode,trvcost=cost,start_date=start_date,boarding_place=boarding_place,place=p,boarding_time=boarding_time,trans=current_user)
                db.session.add(transport)
                db.session.commit()
                flash('Transportation Details is added ')
                return redirect(url_for('payment',_external=True))
            except:
                flash('Transportation could not be added check details Entered')
    

    return render_template('transport.html') 

@app.route('/PayFeed',methods=['GET','POST'])
@login_required
def payment():
    
    if(request.method=='POST'):
        
        pkg=db.session.query(Package).filter(Package.email==current_user.email).all()
        hotl=db.session.query(Hotel).filter(Hotel.email==current_user.email).all()    
        trans=db.session.query(Transport).filter(Transport.email==current_user.email).all() 

        lp,lh,lt,pn=[],[],[],[]  # list for costs

        #iterate through package
        for i in  pkg:
            lp.append(i.estimated_cost)
            pn.append(i.package_name)
            

        #iterate through Hotel
        for i in  hotl:
            lh.append(i.cost)
        
        #iterate through Transport
        for i in  trans:
            lt.append(i.trvcost)
            
        i=0
        while i< len(lh):
            if i==len(lh)-1:
                amt1=int(lp[i])
                amt2= int(lh[i])
                amt3=int(lt[i]) 
                p=pn[i]
                
                break
            else:
                i+=1

        totl = amt1 + amt2 + amt3
        
        
        try:
            ent=Payment(email=current_user.email,total_amount=totl,bookedpack=p,pay=current_user)
            db.session.add(ent)
            db.session.commit()

            flash('Payment Successfully Confirmed')
            return render_template('payment.html',amt1=amt1,amt2=amt2,amt3=amt3,totl=totl)
        except:
            flash('There was problem in making payment')

    pkg=db.session.query(Package).filter(Package.email==current_user.email).all()
    hotl=db.session.query(Hotel).filter(Hotel.email==current_user.email).all()    
    trans=db.session.query(Transport).filter(Transport.email==current_user.email).all() 


    lp,lh,lt=[],[],[]  # list for costs

    #iterate through package
    for i in  pkg:
        lp.append(i.estimated_cost)
        

    #iterate through Hotel
    for i in  hotl:
        lh.append(i.cost)
       


    #iterate through Transport
    for i in  trans:
        lt.append(i.trvcost)
        

    i=0

    while i< len(lh):
        if i==len(lh)-1:
            amt1=int(lp[i])
            amt2= int(lh[i])
            amt3=int(lt[i]) 
            
            break
        else:
            i+=1

    totl = amt1 + amt2 + amt3        
    
    return render_template('payment.html',amt1=amt1,amt2=amt2,amt3=amt3,totl=totl)



@app.route('/feedback',methods=['GET','POST'])

def feedback():
    if(request.method=='POST'):
        name=request.form.get('username')
        email=request.form.get('email')
        scale=request.form.get('scale')
        rate=request.form.get('rating')
        msg=request.form.get('subject')

        try:
            feed=Feedback(username=name,email=email,scale=scale,rating=rate,feedback=msg)
            db.session.add(feed)
            db.session.commit()
            flash('Thank you for the Feedback :) ')
        except:
            flash('There was Problem adding Feedback')        

    return render_template('feedback.html')




@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for('home'))    


@app.route("/travellerInfo")
@login_required
def travellerInfo():
    lp,lh,lt=[],[],[]
    pn,pd=[],[]
    cin,cout=[],[]
    tm,tb,bm=[],[],[]
    pkg=db.session.query(Package).filter(Package.email==current_user.email).all()
    hotl=db.session.query(Hotel).filter(Hotel.email==current_user.email).all() 
    trans=db.session.query(Transport).filter(Transport.email==current_user.email).all() 

    #iterate through package
    for i in  pkg:
        lp.append(i.estimated_cost)
        pn.append(i.package_name)
        pd.append(i.numOfDays)

    #iterate through Hotel
    for i in  hotl:
        lh.append(i.cost)
        cin.append(i.checkin_date)
        cout.append(i.checkout_date)
    


    #iterate through Transport
    for i in  trans:
        lt.append(i.trvcost)
        tm.append(i.mode_of_transport)
        tb.append(i.boarding_place)
        bm.append(i.boarding_time)

    i=0

    while i< len(lh):
        if i==len(lh)-1:
            amt1=int(lp[i])
            amt2= int(lh[i])
            amt3=int(lt[i]) 
            ckot=cout[i]
            ckin=cin[i]
            pname=pn[i]
            pdays=pd[i]
            transb,transm,transt=tb[i],tm[i],bm[i]
            break
        else:
            i+=1

    totl = amt1 + amt2 + amt3 


    
    return render_template('travellerInfo.html',boardp=transb,boardtm=transt,tranmode=transm,ckod=ckot,ckid=ckin,pname=pname,pdays=pdays,user=current_user,total=totl,trans=trans,amt1=amt1,amt2=amt2,amt3=amt3)



if __name__ == "__main__":
    # Wrap db.create_all() within app.app_context() to ensure the application context is set up properly.
    with app.app_context():
        db.create_all()  # This will create all database tables if they don't exist
    app.run(debug=True)