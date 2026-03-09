from datetime import datetime

from app.extensions import db


class Draft(db.Model):
    __tablename__ = "drafts"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    slug = db.Column(db.String(120), nullable=False, unique=True)
    year = db.Column(db.Integer, nullable=False)
    is_active = db.Column(db.Boolean, nullable=False, default=False)
    database_name = db.Column(db.String(120), nullable=False, unique=True)
    num_draft_rounds = db.Column(db.Integer, nullable=False, default=12)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
