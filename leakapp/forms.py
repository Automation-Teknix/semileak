from django import forms
from django.utils.timezone import now
from django.core.exceptions import ValidationError
from leakapp.models import LeakAppMasterData, User, LeakAppTest


class LoginForm(forms.Form):
    username = forms.CharField(
        widget= forms.TextInput(
           attrs={
               "class":'form-contr'
           } 
        )
    )
    password = forms.CharField(
        widget= forms.PasswordInput(
           attrs={
               "class":'form-contr'
           } 
        )
    )
    class Meta:
        model = User
        fields = ('is_admin','username', 'password1' )
        labels = {
            'is_admin': 'TeamAT',
            'username': 'Username',    
        }

    def clean_username(self):
        username = self.cleaned_data['username']
        if User.objects.filter(username=username).exists():
            raise forms.ValidationError('This username already exists. Please choose a different one.')
        return username


class LeakAppMasterDataForm(forms.ModelForm):
    class Meta:
        model = LeakAppMasterData
        fields = "__all__"


class LeakAppTestForm(forms.ModelForm):
    class Meta:
        model = LeakAppTest
        fields = "__all__"
